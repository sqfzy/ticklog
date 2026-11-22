//! The `trace!`, `debug!`, `info!`, `warn!`, and `error!` logging macros and
//! the runtime entry point they expand into.
//!
//! Each macro checks the compile-time level ceiling, and only if the record
//! passes does it evaluate its arguments and call [`dispatch`]. Format-string
//! syntax and the placeholder-to-argument count are checked at compile time via
//! [`check_fmt`](crate::format::check_fmt); per-type specifier validity is
//! resolved at runtime by the drain.

use crate::builder::Backpressure;
use crate::level::Level;
use crate::record;
use crate::thread_buf::with_thread_buf;
use crate::timestamp;

/// Copies a tagged byte payload into the calling thread's log ring.
///
/// The drain delivers the concatenated slices to [`crate::LogSink::accept_bytes`]
/// in the same per-thread order as formatted records. The sink borrows the
/// payload only during that call. Success means the record was enqueued.
/// Like [`crate::try_info!`], this bypasses the logging level ceiling and
/// returns `false` for a full ring, an oversized record or a re-entrant call.
/// The slices may contain up to [`crate::MAX_BYTES_PAYLOAD_SIZE`] bytes in total.
/// Configure the logger and [prewarm the thread](crate::warm_up) before use on
/// a latency-sensitive path.
#[inline]
pub fn try_log_bytes(level: Level, kind: u16, parts: &[&[u8]]) -> bool {
    let mut total_size = record::HEADER_SIZE;
    for part in parts {
        if part.len() > record::MAX_BYTES_RECORD_SIZE - total_size {
            return false;
        }
        total_size += part.len();
    }
    with_thread_buf(|tb| {
        let Some(slot) = tb.ring.reserve(total_size, Backpressure::Drop) else {
            return false;
        };
        // SAFETY: reserve gives the producer exclusive access to this range
        // of initialized ring bytes until publish makes it visible to the drain.
        let dst = unsafe { std::slice::from_raw_parts_mut(slot.ptr, total_size) };
        record::assemble_bytes(dst, level, kind, parts);
        tb.ring.publish(slot);
        true
    })
    .unwrap_or(false)
}

/// The logging macros expand in the caller's crate, where `$crate` is `ticklog`
/// but `crate::record` is unreachable, so they hardcode the fixed-section base
/// record size below. This assertion keeps that literal honest: if the record
/// layout constants change, the build fails here instead of silently mis-sizing
/// every record.
const _: () = assert!(record::BASE_RECORD_SIZE == 41);

/// Monomorphized dispatch: timestamps, assembles the record via
/// [`assemble`](crate::record::assemble), and writes it to the thread's ring
/// buffer.
///
/// The `write_args` closure is monomorphized per unique argument-type
/// signature: the macro expands each `Loggable::type_tag()` /
/// `Loggable::encode()` call as a direct (non-vtable) invocation against the
/// concrete type. `policy` is a compile-time constant from
/// [`configure!`](crate::configure!) and is branch-folded away.
///
/// Not part of the public API.
#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
#[inline(always)]
pub fn dispatch(
    level: Level,
    fmt: &'static str,
    file: &'static str,
    line: u32,
    n_args: u8,
    args_total: usize,
    policy: Backpressure,
    write_args: impl FnOnce(&mut [u8]),
) -> bool {
    with_thread_buf(|tb| {
        let total_size = args_total + tb.thread_section_size as usize;
        // Bound the work done on the producer for a formatted record.
        if total_size > record::MAX_RECORD_SIZE {
            return false;
        }

        if let Some(slot) = tb.ring.reserve(total_size, policy) {
            let timestamp = timestamp::raw_timestamp();
            let flags = record::FLAG_FORMAT | record::FLAG_SOURCE | record::FLAG_THREAD;
            record::assemble(
                slot.ptr,
                level,
                timestamp,
                flags,
                fmt,
                file,
                line,
                tb.thread_id,
                &tb.thread_name,
                n_args,
                total_size,
                write_args,
            );
            tb.ring.publish(slot);
            true
        } else {
            false
        }
    })
    .unwrap_or(false)
}

/// Expands one logging macro. Not part of the public API; use the level
/// macros (`trace!`, `debug!`, `info!`, `warn!`, `error!`).
///
/// The format string is validated at compile time, then the argument values
/// are evaluated only if the level passes the compile-time ceiling.
/// `file!()` and `line!()` capture the outer macro's call site.
///
/// Argument encoding uses monomorphized dispatch: each `Loggable` method
/// call resolves to a concrete impl at compile time with no vtable overhead.
#[doc(hidden)]
#[macro_export]
macro_rules! __ticklog_log {
    ($level:expr, $fmt:literal $(, $arg:expr)* $(,)?) => {{
        if $level <= __ticklog_max_level!() {
            let _ = $crate::__ticklog_try_log!($level, __ticklog_backpressure!(), $fmt $(, $arg)*);
        }
    }};
}

/// Shared encoder for legacy logging and the nonblocking data-log interface.
#[doc(hidden)]
#[macro_export]
macro_rules! __ticklog_try_log {
    // Zero-argument arm: no args to encode, just the fixed record sections.
    ($level:expr, $policy:expr, $fmt:literal $(,)?) => {{
        const _: () = $crate::__private::check_fmt($fmt, 0);
        const __TOTAL: usize = $crate::__private::BASE_RECORD_SIZE;
        $crate::__private::dispatch(
            $level, $fmt, file!(), line!(), 0u8, __TOTAL,
            $policy,
            |_buf| {},
        )
    }};
    // Argument arm: monomorphized per-arg encoding, no vtable.
    ($level:expr, $policy:expr, $fmt:literal, $($arg:expr),+ $(,)?) => {{
        const _: () = $crate::__private::check_fmt(
            $fmt,
            <[&str]>::len(&[$(stringify!($arg)),*]),
        );
        // Arg count is known at compile time: the same value that
        // check_fmt validates.
        const __N_ARGS: u8 =
            <[&str]>::len(&[$(stringify!($arg)),*]) as u8;
        const __BASE: usize = $crate::__private::BASE_RECORD_SIZE;
        // Evaluate each argument expression exactly once, into a cons-list of
        // references. Every later use reads these bindings, so a
        // side-effecting argument runs once rather than once per use.
        let __args = $crate::__ticklog_cons!($($arg),+);
        let __total_size: usize = __BASE
            .wrapping_add(__N_ARGS as usize)
            .wrapping_add($crate::__private::LoggableArgs::args_encoded_size(&__args));
        $crate::__private::dispatch(
            $level, $fmt, file!(), line!(), __N_ARGS, __total_size,
            $policy,
            |__buf: &mut [u8]| {
                // Tags fill buf[0..n_args]; payloads follow.
                let mut __tag: usize = 0;
                let mut __pay: usize = __N_ARGS as usize;
                $crate::__private::LoggableArgs::write_args(
                    &__args, __buf, &mut __tag, &mut __pay,
                );
            },
        )
    }};
}

/// Attempt to enqueue one Info record without waiting for buffer space.
///
/// Returns `true` only after publishing the record to the caller's ring. A full
/// ring, oversized record or re-entrant call returns `false`.
/// This data-log interface always uses `Backpressure::Drop` and bypasses the
/// configured level ceiling. It does not acknowledge file writes or durability.
/// Keep its guard alive and call `warm_up` before entering a latency-sensitive path.
#[macro_export]
macro_rules! try_info {
    ($fmt:literal $(, $arg:expr)* $(,)?) => {
        $crate::__ticklog_try_log!($crate::Level::Info, $crate::Backpressure::Drop, $fmt $(, $arg)*)
    };
}

/// Builds a cons-list of references to the given expressions, evaluating each
/// exactly once: `__ticklog_cons!(a, b)` expands to `(&a, (&b, ()))`.
///
/// Not part of the public API; an implementation detail of the logging macros.
#[doc(hidden)]
#[macro_export]
macro_rules! __ticklog_cons {
    () => { () };
    ($head:expr $(, $tail:expr)*) => {
        (&$head, $crate::__ticklog_cons!($($tail),*))
    };
}

/// Logs a message at [`Level::Trace`].
///
/// Takes a format string literal and positional arguments, e.g.
/// `trace!("state = {}", state)`. The record is discarded unless trace-level
/// logging is enabled.
#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => { $crate::__ticklog_log!($crate::Level::Trace, $($arg)*) };
}

/// Logs a message at [`Level::Debug`].
///
/// Takes a format string literal and positional arguments, e.g.
/// `debug!("value = {}", value)`.
#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => { $crate::__ticklog_log!($crate::Level::Debug, $($arg)*) };
}

/// Logs a message at [`Level::Info`].
///
/// Takes a format string literal and positional arguments, e.g.
/// `info!("listening on {}", port)`.
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => { $crate::__ticklog_log!($crate::Level::Info, $($arg)*) };
}

/// Logs a message at [`Level::Warn`].
///
/// Takes a format string literal and positional arguments, e.g.
/// `warn!("retry {} of {}", n, max)`.
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => { $crate::__ticklog_log!($crate::Level::Warn, $($arg)*) };
}

/// Logs a message at [`Level::Error`].
///
/// Takes a format string literal and positional arguments, e.g.
/// `error!("connection failed: {}", err)`.
#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => { $crate::__ticklog_log!($crate::Level::Error, $($arg)*) };
}
