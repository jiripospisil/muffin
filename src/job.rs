use std::borrow::Cow;

use async_trait::async_trait;
use chrono::Duration;

use crate::{backoff, ProcessResult};

/// This is the trait that needs to implemented for all job structs. The
/// only required methods are [`Job::id()`], which uniquely identifies a job, and
/// [`Job::process()`], which does the actual processing.
///
/// # Example
///
///```no_run
/// use std::sync::Arc;
///
/// use async_trait::async_trait;
/// use mongodb::{bson::doc, Client};
/// use muffin::{Job, ProcessResult, Result, Muffin, Status};
/// use serde::{Deserialize, Serialize};
///
/// struct JobState {
///     // ...
/// }
///
/// #[derive(Serialize, Deserialize)]
/// struct JobA {
///     payload: String,
/// }
///
/// #[async_trait]
/// impl Job for JobA {
///     type State = Arc<JobState>;
///
///     fn id() -> &'static str
///     where
///         Self: Sized,
///     {
///         "JobA"
///     }
///
///     async fn process(&self, state: Self::State) -> ProcessResult {
///         // Do something with self.payload
///         Ok(Status::Completed)
///     }
/// }
/// ```
#[async_trait]
pub trait Job: Send + Sync + 'static {
    /// The type of the variable that gets passed to the [`Job::process`] method.
    type State;

    /// A **unique** identifier of the job. This is used to pair the serialized
    /// job in the database with a handler.
    fn id() -> &'static str
    where
        Self: Sized;

    /// The maximum amount of time the job will be waited for before marking it
    /// as errored/ failed. Defaults to 1 hour.
    ///
    /// Note: Keep in mind that Muffin is only able to check for timeout on
    /// context switch.
    fn timeout(&self) -> Duration {
        Duration::hours(1)
    }

    /// The maximum number of retries before the job is marked as failed.
    fn max_retries(&self) -> u16 {
        5
    }

    /// The delay before a job is processed again after an error.
    fn backoff(&self, retries: u16) -> Duration {
        backoff::exponential(Duration::minutes(2), 3, retries)
    }

    /// The priority of the job. Jobs with lower priority get processed first.
    fn priority(&self) -> i16 {
        500
    }

    /// If provided, no other job with the same unique key can be created until
    /// this one is processed or fails.
    fn unique_key(&self) -> Option<Cow<'static, str>> {
        None
    }

    /// Gets invoked to perform the actual job's work. The return type
    /// [`ProcessResult`] indicates whether the job has finished successfully or might
    /// be retried.
    ///
    /// Note: Keep in mind that the method is invoked in an async context
    /// as any other task and as such it should never block. If you need
    /// to perform a blocking operation or a heavy CPU calculation, use
    /// [`Tokio::spawn_blocking`](https://docs.rs/tokio/latest/tokio/task/index.html#spawn_blocking).
    async fn process(&self, state: Self::State) -> ProcessResult;
}
