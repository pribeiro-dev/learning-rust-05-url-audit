use anyhow::{Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use tokio::time::{timeout, Duration};
use tracing::{info, Level};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(version, about = "Audit a list of URLs from a CSV and output JSON")]
struct Args {
    /// CSV path with a header 'url'
    #[arg(short, long)]
    input: String,

    /// Output JSON path
    #[arg(short, long, default_value = "report.json")]
    output: String,

    /// Max number of concurrent requests
    #[arg(short = 'c', long, default_value_t = 32)]
    concurrency: usize,

    /// Per-request timeout in seconds
    #[arg(short = 't', long, default_value_t = 10u64)]
    timeout: u64,

    /// Optional custom User-Agent header
    #[arg(long, default_value = "url-audit/0.1")]
    user_agent: String,
}

#[derive(Debug, Deserialize)]
struct InRow {
    url: String,
}

#[derive(Debug, Serialize)]
struct OutRow {
    url: String,
    status: Option<u16>,
    len: Option<u64>,
    error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive(Level::INFO.into()))
        .init();

    let args = Args::parse();
    info!("reading {}", &args.input);

    let client = reqwest::Client::builder()
        .user_agent(args.user_agent.clone())
        .tcp_nodelay(true)
        .build()
        .context("building HTTP client")?;

    let mut rdr = csv::Reader::from_path(&args.input)
        .with_context(|| format!("opening CSV: {}", &args.input))?;

    let mut urls: Vec<String> = Vec::new();
    for rec in rdr.deserialize::<InRow>() {
        let row = rec.with_context(|| "parsing CSV row")?;
        if !row.url.trim().is_empty() {
            urls.push(row.url);
        }
    }
    info!(count = urls.len(), "loaded URLs");

    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(args.concurrency));
    let mut tasks = Vec::with_capacity(urls.len());
    for url in urls {
        let client = client.clone();
        let permit = sem.clone().acquire_owned().await?;
        let tmo = Duration::from_secs(args.timeout);
        tasks.push(tokio::spawn(async move {
            let _permit = permit;
            fetch_row(&client, url, tmo).await
        }));
    }

    let mut out = Vec::with_capacity(tasks.len());
    for t in tasks {
        match t.await {
            Ok(row) => out.push(row),
            Err(e) => out.push(OutRow {
                url: "<join-error>".into(),
                status: None,
                len: None,
                error: Some(format!("join error: {e}")),
            }),
        }
    }

    std::fs::write(&args.output, serde_json::to_vec_pretty(&out)?)
        .with_context(|| format!("writing {}", &args.output))?;

    info!("wrote {} rows to {}", out.len(), &args.output);
    Ok(())
}

async fn fetch_row(client: &reqwest::Client, url: String, tmo: Duration) -> OutRow {
    // Keep one clone for the timeout case
    let url_for_timeout = url.clone();

    let fut = async {
        match client.get(&url).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let len = resp
                    .headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok());
                OutRow {
                    url,
                    status: Some(status),
                    len,
                    error: None,
                }
            }
            Err(e) => OutRow {
                url,
                status: None,
                len: None,
                error: Some(e.to_string()),
            },
        }
    };

    match tokio::time::timeout(tmo, fut).await {
        Ok(row) => row,
        Err(_) => OutRow {
            url: url_for_timeout,
            status: None,
            len: None,
            error: Some("timeout".into()),
        },
    }
}
