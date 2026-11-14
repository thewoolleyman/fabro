//! A stage's sandbox lost under it ends the run's lifetime, not the run.
//!
//! When the sandbox provider no longer has a stage's sandbox (a Kubernetes
//! pod deleted mid-stage, a Docker container removed), every command the
//! stage's checkpoint commit runs in it fails with the provider's typed
//! not-found, rendered as `Sandbox "<id>" was not found`. Nothing about the
//! work item caused that, and the workspace may well have survived: the
//! Kubernetes provider keeps it on a node-local `hostPath` and re-creates
//! the pod over it when the lease is acquired again. So the run must not
//! end as a failed checkpoint, which recovery refuses to resume.
//!
//! Petri's hook vocabulary has no decision that ends a run's lifetime
//! without recording the attempt, but a failed write to the run's store
//! does exactly that: Petri records nothing after it, the coordinator
//! returns `CoordinatorError::StoreFailed`, and the next lifetime resumes
//! from what the store holds. [`SandboxLoss`] uses that path. Fabro's hooks
//! [`trip`](SandboxLoss::trip) it when a checkpoint commit meets a lost
//! sandbox, BEFORE returning the stage's result, and the store
//! [`fence`](SandboxLoss::fence) wraps refuses every write from then on. The
//! stage's finish is the first record refused, so the stage the loss
//! interrupted has no end in the store and runs again on the resume, while
//! every stage that finished before the loss keeps its record and does not.
//!
//! The engine then reports the lifetime's end as a lost sandbox rather than
//! as a failed store, so the server resumes the run under its own count of
//! sandbox losses.

use std::sync::{Arc, OnceLock};

use petri_store::{Access, Digest, LogId, Record, RunKey, RunLogs, RunStore, StoreError};

/// How the sandbox provider renders a sandbox it does not have: the
/// `Display` of sandbox-driver's `Error::NotFound` for a sandbox, which
/// reaches a checkpoint commit as the cause of its failed command.
const NOT_FOUND_PREFIX: &str = "Sandbox \"";
const NOT_FOUND_SUFFIX: &str = "\" was not found";

/// Whether a failure message says the sandbox provider no longer has the
/// sandbox a command ran in.
#[must_use]
pub fn names_lost_sandbox(message: &str) -> bool {
    message.match_indices(NOT_FOUND_PREFIX).any(|(at, _)| {
        let rest = &message[at + NOT_FOUND_PREFIX.len()..];
        rest.find(NOT_FOUND_SUFFIX)
            .is_some_and(|end| end > 0 && !rest[..end].contains('"'))
    })
}

/// One run lifetime's lost sandbox: the first loss Fabro's hooks met, once
/// they met one. Tripping it fences the run's store.
#[derive(Debug, Default)]
pub struct SandboxLoss {
    lost: OnceLock<String>,
}

impl SandboxLoss {
    /// Record a lost sandbox; only the first is kept. From now on the
    /// fenced store refuses every write.
    pub fn trip(&self, message: String) {
        let _ = self.lost.set(message);
    }

    /// The first lost sandbox's failure, once there is one.
    #[must_use]
    pub fn lost(&self) -> Option<&str> {
        self.lost.get().map(String::as_str)
    }

    /// `inner`, refusing every write once this loss is tripped. Reads pass
    /// through, so the run's record stays readable.
    #[must_use]
    pub fn fence(self: &Arc<Self>, inner: Arc<dyn RunStore>) -> Arc<dyn RunStore> {
        Arc::new(FencedStore {
            inner,
            loss: Arc::clone(self),
        })
    }
}

/// A run store whose runs refuse writes once the run's sandbox is lost.
struct FencedStore {
    inner: Arc<dyn RunStore>,
    loss:  Arc<SandboxLoss>,
}

#[async_trait::async_trait]
impl RunStore for FencedStore {
    async fn open(&self, key: &RunKey, access: Access) -> Result<Arc<dyn RunLogs>, StoreError> {
        let logs = self.inner.open(key, access).await?;
        Ok(Arc::new(FencedLogs {
            inner: logs,
            loss:  Arc::clone(&self.loss),
        }))
    }
}

struct FencedLogs {
    inner: Arc<dyn RunLogs>,
    loss:  Arc<SandboxLoss>,
}

impl FencedLogs {
    /// The refusal of a write once the sandbox is lost: a backend failure,
    /// which ends the lifetime as any failed store write does.
    fn refuse(&self, action: &'static str) -> Result<(), StoreError> {
        if let Some(lost) = self.loss.lost() {
            return Err(StoreError::backend(
                self.inner.locator(),
                action,
                format!("the run's sandbox was lost: {lost}"),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl RunLogs for FencedLogs {
    fn locator(&self) -> String {
        self.inner.locator()
    }

    async fn append(&self, log: &LogId, records: &[Record]) -> Result<(), StoreError> {
        self.refuse("append")?;
        self.inner.append(log, records).await
    }

    async fn read(&self, log: &LogId) -> Result<Vec<Record>, StoreError> {
        self.inner.read(log).await
    }

    async fn read_from(&self, log: &LogId, seq: u64) -> Result<Vec<Record>, StoreError> {
        self.inner.read_from(log, seq).await
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<Digest, StoreError> {
        self.refuse("put_blob")?;
        self.inner.put_blob(bytes).await
    }

    async fn get_blob(&self, digest: Digest) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get_blob(digest).await
    }
}

#[cfg(test)]
mod tests {
    use super::names_lost_sandbox;

    #[test]
    fn the_provider_not_found_names_a_lost_sandbox() {
        // Measured 2026-11-13, run 01M7A7G3APRTWPMSAS30MYAB7K on gmktec.
        assert!(names_lost_sandbox(
            "the checkpoint commit of `implement` failed: git rev-parse failed (wait failed): \
             sandbox exec failed: Sandbox \"petri-01m7a7g3aprtwpmsas30myab7k-l0\" was not found"
        ));
    }

    #[test]
    fn other_checkpoint_failures_do_not_name_a_lost_sandbox() {
        for message in [
            "the checkpoint commit of `wreck` failed: git rev-parse failed (exit status: 128): \
             fatal: invalid gitfile format: .git",
            "the checkpoint commit of `x` failed: git add failed: the file was not found",
            "Sandbox \"\" was not found",
            "sandbox exec failed: reaching the Kubernetes API failed: client error (Connect)",
        ] {
            assert!(!names_lost_sandbox(message), "{message}");
        }
    }
}
