use std::{io::Write, sync::LazyLock};

use anyhow::Result;
use dashmap::DashSet;
use hashbrown::HashSet;
use regex::Regex;
use reqwest::Url;
use tokio::sync::{Semaphore, SemaphorePermit, mpsc::UnboundedSender};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::prelude::*;

static SEMAPHORE: Semaphore = Semaphore::const_new(1); // raise at your own risk
static VISITED: LazyLock<DashSet<Url>> = LazyLock::new(DashSet::new);
static SEEN: LazyLock<DashSet<Url>> = LazyLock::new(DashSet::new);
static CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (compatible; Rust crawler)")
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("Failed to create HTTP client")
});

static ABS_REGEX: LazyLock<[Regex; 1]> =
    LazyLock::new(|| [Regex::new(r"https?://(?:[\w\d-]+\.)+\w+(?:/[\w\d\-._~?#&]+)*/?").unwrap()]);
static REL_REGEX: LazyLock<[Regex; 1]> =
    LazyLock::new(|| [Regex::new(r#"(?i)<a\s+[^>]*href\s*=\s*['"]([^'"]+)['"][^>]*>"#).unwrap()]);

#[tracing::instrument(skip_all, fields(base = %base))]
fn parse_links(base: &Url, body: String) -> HashSet<Url> {
    // let absolute_patterns = [r"https?://(?:[\w\d-]+\.)+\w+(?:/[\w\d\-._~?#&]+)*/?"];
    // let relative_patterns = [r#"(?i)<a\s+[^>]*href\s*=\s*['"]([^'"]+)['"][^>]*>"#]; // it's a little evil
    let mut urls = HashSet::new();

    for re in ABS_REGEX.iter() {
        urls.extend(
            re.find_iter(&body)
                .filter_map(|m| Url::parse(m.as_str()).ok()),
        );
    }

    for re in REL_REGEX.iter() {
        urls.extend(re.captures_iter(&body).filter_map(|cap| {
            let relative = cap.get(1)?.as_str();
            base.join(relative).ok()
        }));
    }

    urls.retain(|u| !SEEN.contains(u) && SEEN.insert(u.clone()));

    urls
}

#[tracing::instrument(skip_all, fields(url = %url))]
async fn crawl(url: Url) -> Result<impl Iterator<Item = Url>> {
    tracing::debug!("crawling URL");
    let response = CLIENT.get(url).send().await?;
    VISITED.insert(response.url().to_owned());
    let base = response.url().to_owned();
    let body = response.text().await?;
    let links = parse_links(&base, body);
    Ok(links.into_iter().filter(|link| !VISITED.contains(link)))
}

fn setup_tracing() {
    let filter = tracing_subscriber::filter::EnvFilter::new("info,links=debug");
    let fmt = tracing_subscriber::fmt::layer()
        .compact()
        .with_thread_ids(true)
        .with_thread_names(true);

    tracing_subscriber::registry().with(fmt).with(filter).init();
}

#[tokio::main]
async fn main() {
    setup_tracing();

    let token = CancellationToken::new();
    let handle = token.clone();

    let (qtx, mut qrx) = tokio::sync::mpsc::unbounded_channel();

    // cancel on Ctrl-C
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("ctrl-c received, cancelling...");
        handle.cancel();
        SEMAPHORE.close();
    });

    let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(interval);

    let url = Url::parse("https://en.wikipedia.org/wiki/Main_Page").unwrap();
    qtx.send(url).expect("Failed to send initial URL");

    let task = async |_permit: SemaphorePermit,
                      url: Url,
                      qtx: UnboundedSender<Url>,
                      token: CancellationToken| {
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                tracing::info!("task cancelled, exiting...");
            }
            res = crawl(url.clone()) => {
                match res {
                    Err(e) => {
                        tracing::error!(cause = %e, "failed to crawl URL");
                        if !VISITED.contains(&url) {
                            // send the URL back to the queue
                            tracing::warn!("re-queueing URL: {url}");
                            let _ = qtx.send(url);
                        }
                    }
                    Ok(res) => {
                        res.for_each(|l| {
                            let _ = qtx.send(l);
                        });
                    }
                }
            }
        }
    };

    loop {
        tokio::select! {
            biased;
            _ = token.cancelled(), if !qrx.is_empty() => {
                tracing::info!("cancellation requested, exiting...");
                break;
            }
            _ = interval.tick() => {
                tracing::info!("queue: {}, visited: {}, seen: {}", qrx.len(), VISITED.len(), SEEN.len());
            }
            Some(url) = qrx.recv() => {
                if !VISITED.contains(&url) && let Ok(permit) = SEMAPHORE.acquire().await {
                    tokio::spawn(task(permit, url, qtx.clone(), token.clone()));
                }
            }
            else => {
                tracing::info!("no more URLs to process.");
                break;
            }
        }
    }

    // clean up
    if !qrx.is_empty() {
        tracing::info!("processing remaining URLs in queue...");
        let writer = std::fs::File::create("links.txt").expect("failed to create file");
        let mut writer = std::io::BufWriter::new(writer);
        qrx.close();
        while let Some(url) = qrx.recv().await {
            let _ = writeln!(writer, "{url}",);
        }
        writer.flush().unwrap();
        tracing::info!("links saved to links.txt");
    }

    tracing::info!("crawling completed, total visited: {}", VISITED.len());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse() {
        let html = r#"
            <html>
                <head><title>Test</title></head>
                <body>
                    <a href="https://example.com">Example</a>
                    <a href="https://aaa.rust-lang.org">Rust</a>
                    <a href="/relative/path">Relative</a>
                </body>
            </html>
        "#;

        let urls = parse_links(&Url::parse("https://google.com").unwrap(), html.to_owned());
        let expected: HashSet<_> = [
            "https://example.com",
            "https://aaa.rust-lang.org",
            "https://google.com/relative/path",
        ]
        .iter()
        .map(|s| Url::parse(s).unwrap())
        .collect();

        let actual: HashSet<_> = urls.into_iter().collect();
        assert_eq!(actual, expected);
    }
}
