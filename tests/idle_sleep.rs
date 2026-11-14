//! Producers first seen after an idle sleep must be discovered and flushed
//! without waiting for 1024 polls or for Guard shutdown.
use std::{
    io,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::Duration,
};

use ticklog::{Level, LogSink};

type Flushed = Arc<(Mutex<Vec<String>>, Condvar)>;

struct BufferedSink {
    pending: Vec<String>,
    flushed: Flushed,
}

impl LogSink for BufferedSink {
    fn accept(&mut self, line: &[u8], _: Level) -> io::Result<()> {
        self.pending.push(String::from_utf8(line.to_vec()).unwrap());
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flushed.0.lock().unwrap().append(&mut self.pending);
        self.flushed.1.notify_all();
        Ok(())
    }
}

fn wait_for(flushed: &Flushed, message: &str) {
    let (lines, timeout) = flushed
        .1
        .wait_timeout_while(
            flushed.0.lock().unwrap(),
            Duration::from_millis(200),
            |lines| !lines.iter().any(|line| line == message),
        )
        .unwrap();
    assert!(
        !timeout.timed_out(),
        "record was not flushed after idle: {lines:?}"
    );
}

#[test]
fn new_live_and_exited_producers_are_flushed_after_idle() {
    let flushed: Flushed = Arc::new((Mutex::new(Vec::new()), Condvar::new()));
    let guard = ticklog::configure! {
        sink: BufferedSink { pending: Vec::new(), flushed: flushed.clone() },
        format: "{message}",
    }
    .unwrap();
    thread::sleep(Duration::from_millis(10));
    thread::spawn(|| assert!(ticklog::try_info!("exited producer")))
        .join()
        .unwrap();
    wait_for(&flushed, "exited producer");

    thread::sleep(Duration::from_millis(10));
    assert!(ticklog::try_info!("live producer"));
    wait_for(&flushed, "live producer");

    assert!(ticklog::try_info!("final record"));
    drop(guard);
    let lines = flushed.0.lock().unwrap();
    assert_eq!(
        lines.as_slice(),
        ["exited producer", "live producer", "final record"]
    );
}
