use std::sync::Arc;

use async_trait::async_trait;
use mongodb::{bson::doc, Client};
use muffin::{Job, Muffin, ProcessResult, Result, Status};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

struct JobState {
    total: usize,
}

#[derive(Serialize, Deserialize)]
struct JobA {
    number: usize,
}

#[async_trait]
impl Job for JobA {
    type State = Arc<Mutex<JobState>>;

    fn id() -> &'static str
    where
        Self: Sized,
    {
        // A unique identifier of the job
        "JobA"
    }

    async fn process(&self, state: Self::State) -> ProcessResult {
        state.lock().total += self.number;
        Ok(Status::Completed)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to a database called "muffin_testing".
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let database = client.database("muffin_testing");

    // The state is passed to each "process" method on an instance of Job. Jobs
    // can be processed on multiple threads so we need to wrap the state with a
    // Mutex if we want to mutate it.
    let state = Arc::new(Mutex::new(JobState { total: 0 }));
    let muffin = Muffin::new(database, state.clone()).await?;

    // Push two jobs
    muffin.push(JobA { number: 42 }).await?;
    muffin.push(JobA { number: 13 }).await?;

    // Register jobs that should be processed.
    muffin.register::<JobA>().await;

    // Process 2 jobs and for them to finish.
    muffin.process_n(2).await?;

    // Check that the state got mutated as expected.
    assert_eq!(55, state.lock().total);

    Ok(())
}
