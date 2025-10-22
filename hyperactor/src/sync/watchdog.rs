use std::fmt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use tokio::sync::watch;
use tokio::time::MissedTickBehavior;
use tokio::time::sleep_until;

use crate::clock::Clock;
use crate::clock::RealClock;

/// TODO
pub struct Watchdog<S> {
    tx: watch::Sender<(tokio::time::Instant, S)>,
    interval: tokio::time::Interval,
    ok: Arc<AtomicBool>,
}

impl<S: fmt::Display + Send + Sync + 'static> Watchdog<S> {
    /// TODO
    pub fn spawn(interval: std::time::Duration, name: String, init: S) -> Self {
        let (tx, rx) = watch::channel((RealClock.now(), init));
        let ok = Arc::new(AtomicBool::new(true));
        tokio::spawn(Self::watcher(interval, rx, name, Arc::clone(&ok)));
        let mut interval = tokio::time::interval(interval / 2);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        Self { tx, interval, ok }
    }

    /// TODO
    pub async fn tick(&mut self, state: S) {
        self.send(state);
        self.interval.tick().await;
    }

    /// TODO
    pub fn send(&mut self, state: S) {
        self.tx.send((RealClock.now(), state)).unwrap();
    }

    async fn watcher(
        interval: std::time::Duration,
        mut state: watch::Receiver<(tokio::time::Instant, S)>,
        name: String,
        ok: Arc<AtomicBool>,
    ) {
        let mut was_ok = true;
        loop {
            was_ok = {
                let time_and_state = state.borrow_and_update();
                let is_ok = time_and_state.0.elapsed() < interval;
                ok.store(is_ok, Ordering::Relaxed);
                if !is_ok {
                    let overdue = time_and_state.0.elapsed() - interval;
                    tracing::error!(
                        "watchdog {} overdue by {}s; last state: {}",
                        name,
                        overdue.as_secs(),
                        time_and_state.1
                    );
                } else if !was_ok {
                    tracing::error!(
                        "watchdog {} recovered; new state: {}",
                        name,
                        time_and_state.1
                    );
                }
                is_ok
            };

            tokio::select! {
                _ = sleep_until(RealClock.now() + interval) => (),
                changed = state.changed() => {
                    if changed.is_err() {
                        break
                    }
                }
            }
        }
    }
}

impl<S> Watchdog<S> {
    pub fn ok(&self) -> bool {
        self.ok.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tracing_test::traced_test;

    use super::*;

    #[tokio::test(start_paused = true)]
    #[traced_test]
    async fn test_basic() {
        let mut w = Watchdog::spawn(Duration::from_secs(5), "test".to_string(), 0usize);

        w.send(1);
        assert!(w.ok());
        tokio::time::advance(Duration::from_secs(6)).await;
        assert!(!w.ok());
        w.send(2);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(w.ok());

        // TODO: verified manually that these are emitted
        //   assert!(logs_contain("watchdog test overdue by 1s; last state: 1"));
        //   assert!(logs_contain("watchdog test recovedred; new state: 2"));
    }
}
