//! Oversized formatted records are rejected without interrupting the log stream.

use std::sync::{Arc, Mutex};

use ticklog::{Level, LogSink, info};

/// A sink that records every accepted line, for assertions after shutdown.
struct CaptureSink {
    lines: Arc<Mutex<Vec<String>>>,
}

impl LogSink for CaptureSink {
    fn accept(&mut self, line: &[u8], _level: Level) -> std::io::Result<()> {
        self.lines
            .lock()
            .unwrap()
            .push(String::from_utf8_lossy(line).into_owned());
        Ok(())
    }
}

#[test]
fn oversized_record_is_dropped_and_next_record_survives() {
    let lines = Arc::new(Mutex::new(Vec::new()));
    let sink = CaptureSink {
        lines: Arc::clone(&lines),
    };

    let guard = ticklog::configure! {
        sink: sink,
        max_level: Level::Trace,
    }
    .expect("first configure in a fresh process must succeed");

    // The encoded record exceeds the formatted record limit.
    let huge = "x".repeat(70_000);
    info!("huge={}", huge);

    // A normal record immediately after. It must reach the sink uncorrupted --
    // that is the proof the oversized record did not desync the ring.
    info!("sentinel={}", 42u64);

    // Flush and join the drain.
    drop(guard);

    let captured = lines.lock().unwrap();

    // The sentinel survives, byte-for-byte.
    assert!(
        captured.iter().any(|l| l.ends_with("sentinel=42")),
        "sentinel record must survive the oversized record; got {captured:?}"
    );

    // The oversized record is dropped, not partially emitted.
    assert!(
        captured.iter().all(|l| !l.contains("huge=")),
        "oversized record must be dropped, not emitted; got {captured:?}"
    );
}
