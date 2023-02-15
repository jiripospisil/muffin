//! Muffin is a background job processing library designed to work with MongoDB as its storage backend.
//!
//! *Requires MongoDB 6 with feature compatibility level set to `6`.*
//!
//! # Features
//! - Asynchronous processing using [`Tokio`](https://docs.rs/tokio/latest/tokio/)
//! - Job objects serialized with [`Serde`](https://docs.rs/serde/latest/serde/)
//! - [`Scheduled jobs`](Muffin::schedule())
//! - [`Prioritized jobs`](Job::priority())
//! - Retries with [`custom backoff strategies`](Job::backoff())
//! - [`Unique jobs`](Job::unique_key())
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use async_trait::async_trait;
//! use mongodb::{bson::doc, Client};
//! use muffin::{Job, ProcessResult, Muffin, Status};
//! use serde::{Deserialize, Serialize};
//!
//! struct JobState {
//!     mongo_client: Client,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct MyJob {
//!     name: String,
//! }
//!
//! #[async_trait]
//! impl Job for MyJob {
//!     type State = Arc<JobState>;
//!
//!     fn id() -> &'static str
//!     where
//!        Self: Sized,
//!     {
//!         // A unique identifier of the job
//!         "MyJob"
//!     }
//!
//!     async fn process(&self, state: Self::State) -> ProcessResult {
//!        // Do something with self.name and state
//!        Ok(Status::Completed)
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> muffin::Result<()> {
//!     // Connect to a database called "muffin_testing".
//!     let client = Client::with_uri_str("mongodb://localhost:27017").await?;
//!     let database = client.database("muffin_testing");
//!
//!     // The state is passed to each "process" method on an instance of Job.
//!     let state = JobState {
//!         mongo_client: client,
//!     };
//!     let mut muffin = Muffin::new(database, Arc::new(state)).await?;
//!
//!     // Create a new job and push it for processing
//!     muffin
//!         .push(MyJob {
//!             name: "Peter".into(),
//!         })
//!         .await?;
//!
//!     // Register jobs that should be processed.
//!     muffin.register::<MyJob>();
//!
//!     // Start processing jobs.
//!     tokio::select! {
//!         _ = muffin.run() => {},
//!         _ = tokio::signal::ctrl_c() => {
//!             eprintln!("Received ctrl+c!");
//!         }
//!     };
//!
//!     // Need to wait for all in-flight jobs to finish processing.
//!     muffin.shutdown().await;
//!
//!     Ok(())
//! }
//!```
//!
//! You can find other examples in the [`repository`](https://github.com/jiripospisil/muffin/tree/master/examples).

pub mod backoff;
mod config;
mod db;
mod job;
mod muffin;
mod types;

pub use crate::muffin::Muffin;
pub use config::{Config, ConfigBuilder};
pub use job::Job;
pub use types::{Error, JobPersistedStatus, ProcessResult, Result, Schedule, Status};
