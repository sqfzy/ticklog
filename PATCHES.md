# Fork changes

Base: crates.io ticklog 0.1.2, commit `65350658528cbe5267d5dd5bb56e164442e62e40`.
Original file hashes are in `upstream.json`. MIT and Apache-2.0 licenses are retained.
These changes were migrated from Factorbrew's vendored copy. The fork retains
the upstream examples, benchmarks, and their development dependencies.

`try_info!` shares the existing encoder and returns whether a complete record was
enqueued. It always uses Drop backpressure, does not apply the ordinary diagnostic
level ceiling, and does not acknowledge disk writes. Existing logging macros keep
their original behavior and unit return value. The app counts rejected records
on the producer thread, without shared atomic increments for each attempted log.

The drain flushes pending output and sleeps for 1 ms when every queue is empty,
instead of busy-spinning. On wakeup it refreshes the producer registry immediately;
the 1024-poll registry cadence still applies during continuous draining. Records
from already-exited producers also mark the sink dirty, so they are flushed before
the next sleep. Producer enqueue/backpressure behavior is unchanged. Delivery and
shutdown can incur the sleep plus OS scheduling delay.

`tests/idle_sleep.rs` checks that live and exited producers created after idle
are discovered and flushed without waiting for shutdown, and that the final
record is drained when the guard is dropped.

Each producer's ring is 64 MiB by default. Window's production factor batches
need room for bursts and delayed background draining. Capacity is fixed, with
no Cargo feature to select it. Nonblocking Drop backpressure is unchanged;
the larger queue does not guarantee lossless delivery to a slow sink.
