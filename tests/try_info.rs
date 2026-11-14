use std::{
    io,
    sync::{Arc, Condvar, Mutex, mpsc},
    time::Duration,
};
use ticklog::{Level, LogSink};

struct PausedSink {
    gate: Arc<(Mutex<bool>, Condvar)>,
    lines: Arc<Mutex<Vec<String>>>,
}
impl LogSink for PausedSink {
    fn accept(&mut self, line: &[u8], _: Level) -> io::Result<()> {
        let (lock, wake) = &*self.gate;
        let guard = lock.lock().unwrap();
        drop(wake.wait_while(guard, |open| !*open).unwrap());
        self.lines
            .lock()
            .unwrap()
            .push(String::from_utf8(line.to_vec()).unwrap());
        Ok(())
    }
}

#[test]
fn enqueue_results_match_delivered_records_without_waiting_for_the_sink() {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let lines = Arc::new(Mutex::new(Vec::new()));
    let guard = ticklog::configure! {
        sink: PausedSink { gate: gate.clone(), lines: lines.clone() },
        max_level: Level::Error,
        backpressure: ticklog::Backpressure::Block,
        format: "{message}",
    }
    .unwrap();
    let (tx, rx) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        ticklog::warm_up().unwrap();
        assert!(!ticklog::try_info!("{}", "x".repeat(70_000)));
        let mut accepted = Vec::new();
        let mut rejected = 0;
        for seq in 0..200_000_u64 {
            if ticklog::try_info!("{}", seq) {
                accepted.push(seq.to_string());
            } else {
                rejected += 1;
            }
        }
        tx.send((accepted, rejected)).unwrap();
    });
    // Release the sink even if the producer accidentally blocks, so failure
    // terminates rather than leaving a hung test process.
    let result = rx.recv_timeout(Duration::from_secs(5));
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    worker.join().unwrap();
    let (accepted, rejected) = result.expect("producer waited for the sink");
    assert!(rejected > 0 && !accepted.is_empty());
    assert_eq!(accepted.len() + rejected, 200_000);
    drop(guard);
    assert_eq!(*lines.lock().unwrap(), accepted);
}
