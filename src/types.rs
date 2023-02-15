use std::{
    borrow::Cow,
    fmt::{self, Debug, Display},
};

use chrono::{DateTime, Duration, Utc};
use mongodb::bson::oid::ObjectId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::AcquireError;

use crate::Job;

#[derive(Debug)]
pub enum ProcessJobsResult {
    Processed(LockedJob, Status),
    NotFound,
    Error(Error),
}

/// Status of the job as returned from the `process` method on an instance of Job.
#[derive(Debug)]
pub enum Status {
    /// The job has completed and can now be removed (depending on the configuration).
    Completed,

    /// The job has encountered an error. The job will be retried according its retry policy.
    Error(Cow<'static, str>),

    /// The job has encountered an error. The job will be retried according its
    /// retry policy.
    ///
    /// This is the same as regular [`Status::Error`] but allows to return a
    /// `std::error::Error`. This status is automatically used if you use the ?
    /// operator.
    ErrorObj(Box<dyn std::error::Error + Send + Sync>),

    /// The job has encountered an error and will NOT be tried again.
    Fail(Cow<'static, str>),

    /// The job will be tried again in the future. This attempt is still
    /// counted towards the total number of retries.
    RetryIn(Duration),
}

pub type Result<T> = std::result::Result<T, Error>;

/// The returned type of the [`Job::process`] method. See the [`Status`]
/// for more information. Because this is internally just a standard Result,
/// you can use the ? operator (in case of Error, the job will be assigned
/// [`Status::ErrorObj`]).
pub type ProcessResult = std::result::Result<Status, Box<dyn std::error::Error + Send + Sync>>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("configuration error")]
    Config(&'static str),

    #[error("error from MongoDB")]
    Mongo(#[from] mongodb::error::Error),

    #[error("error while serializing/deserializing BSON")]
    Bson(#[from] mongodb::bson::ser::Error),

    #[error("error while working with time")]
    Time(&'static str),

    #[error("IO error")]
    Io(#[from] std::io::Error),

    #[error("serde_json error")]
    Deserializer(#[from] serde_json::Error),

    #[error("Tokio sync error")]
    Acquire(#[from] AcquireError),
}

pub type DeserializerFn<S> =
    Box<dyn Fn(Value) -> Result<Box<dyn Job<State = S>>> + Send + Sync + 'static>;

/// The Job's status as persisted in the database.
#[derive(Debug, Serialize, Deserialize)]
pub enum JobPersistedStatus {
    /// The job is waiting to be processed (`process_at`).
    Waiting,

    /// The job is currently being processed.
    Processing,

    /// The job has been successfully processed. Completed jobs are
    /// automatically deleted. See [`crate::Config`] for more information.
    Completed,

    /// The job has encountered an error and will be retried in the future.
    Errored,

    /// The job has failed and will NOT be tried again.
    Failed,
}

impl Display for JobPersistedStatus {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        Debug::fmt(self, f)
    }
}

/// Used when scheduling jobs for alter processing using the
/// [`crate::Muffin::schedule`] method. Note that the library polls for available jobs and
/// as such jobs might not be executed precisely at the scheduled time.
#[derive(Debug)]
pub enum Schedule {
    /// Schedule the job to be executed at the given time.
    At(DateTime<Utc>),

    /// Schedule the job to be executed this far in the future.
    In(Duration),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LockedJob {
    pub _id: ObjectId,
    pub r#type: String,
    pub attempt_count: u16,
    pub payload: Value,
}
