//! Controlled fixture delays use completion timers on the calling thread.

use liburing::{
    Completion, OperationId, Proactor, ProactorConfig, SubmissionContext, ops::Timeout,
};
use std::{io, time::Duration};

struct Wait {
    ring: Proactor,
    operand: Option<Timeout>,
}

impl Drop for Wait {
    fn drop(&mut self) {
        if self.ring.shutdown().is_err()
            && let Some(operand) = self.operand.take()
        {
            // Failed draining cannot prove the kernel released its timespec.
            std::mem::forget(operand);
        }
    }
}

/// Waits for a controlled fixture interval through a kernel completion timer.
///
/// # Errors
/// Returns ring initialization, submission, completion, or timeout setup errors.
pub fn pause(duration: Duration) -> io::Result<()> {
    let mut wait = Wait {
        ring: Proactor::new(ProactorConfig {
            entries: 8,
            staged_capacity: 8,
            max_cqe_batch: 8,
            ..ProactorConfig::default()
        })
        .map_err(io::Error::other)?,
        operand: Some(Timeout::new(duration).map_err(io::Error::other)?),
    };
    let timer = wait
        .operand
        .as_ref()
        .ok_or_else(|| io::Error::other("missing timer operand"))?;
    let id = OperationId::from_raw((3 << 56) | 1);
    wait.ring
        .prepare_sqe_with(id, |sqe| {
            // SAFETY: Exclusive SQE; the guard retains immutable timeout storage
            // until its final CQE, successful shutdown, or permanent quarantine.
            unsafe { timer.prepare(sqe) };
        })
        .map_err(io::Error::other)?;
    let mut finished = false;
    while !finished {
        wait.ring
            .wait_completions(
                1,
                None,
                &mut |completion: Completion, _: &mut SubmissionContext<'_>| {
                    if completion.operation_id == id {
                        if completion.result != -libc::ETIME {
                            return Err(liburing::UringError::syscalls(
                                "fixture timer",
                                completion.result,
                            ));
                        }
                        finished = true;
                    }
                    Ok(())
                },
            )
            .map_err(io::Error::other)?;
    }
    Ok(())
}
