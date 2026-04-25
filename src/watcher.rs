use notify::{Config as NotifyConfig, Event, RecommendedWatcher, RecursiveMode, Watcher as _};
use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;

pub fn spawn(
    root: PathBuf,
    debounce_ms: u64,
    poll: bool,
    tx: UnboundedSender<()>,
) -> io::Result<thread::JoinHandle<()>> {
    if poll {
        Ok(thread::spawn(move || run_polling(debounce_ms, tx)))
    } else {
        spawn_watcher(root, debounce_ms, tx)
    }
}

fn spawn_watcher(
    root: PathBuf,
    debounce_ms: u64,
    tx: UnboundedSender<()>,
) -> io::Result<thread::JoinHandle<()>> {
    let (watch_tx, watch_rx) = mpsc::channel();
    let mut watcher = RecommendedWatcher::new(
        move |result| {
            let _ = watch_tx.send(result);
        },
        NotifyConfig::default(),
    )
    .map_err(io::Error::other)?;

    watcher
        .watch(&root, RecursiveMode::Recursive)
        .map_err(io::Error::other)?;

    let debounce = Duration::from_millis(debounce_ms.max(20));
    Ok(thread::spawn(move || {
        let _watcher = watcher;
        while let Ok(result) = watch_rx.recv() {
            match result {
                Ok(event) => {
                    tracing::debug!("filesystem event: {:?}", event.kind);
                    drain_debounce_window(&watch_rx, debounce);
                    let _ = tx.send(());
                }
                Err(err) => tracing::warn!("filesystem watch error: {err}"),
            }
        }
    }))
}

fn drain_debounce_window(rx: &mpsc::Receiver<notify::Result<Event>>, debounce: Duration) {
    let deadline = Instant::now() + debounce;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }

        match rx.recv_timeout(remaining) {
            Ok(Ok(event)) => tracing::debug!("filesystem event: {:?}", event.kind),
            Ok(Err(err)) => tracing::warn!("filesystem watch error: {err}"),
            Err(mpsc::RecvTimeoutError::Timeout) => return,
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
}

fn run_polling(interval_ms: u64, tx: UnboundedSender<()>) {
    let interval = Duration::from_millis(interval_ms.max(50));
    loop {
        thread::sleep(interval);
        if tx.send(()).is_err() {
            return;
        }
    }
}
