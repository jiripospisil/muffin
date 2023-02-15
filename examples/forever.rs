use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use mongodb::{bson::doc, Client};
use muffin::{Config, Job, Muffin, ProcessResult, Result, Status};
use serde::{Deserialize, Serialize};
use tokio::time;

struct JobState {}

#[derive(Serialize, Deserialize)]
struct JobA {
    num: u32,
}

#[async_trait]
impl Job for JobA {
    type State = Arc<JobState>;

    fn id() -> &'static str
    where
        Self: Sized,
    {
        // A unique identifier of the job
        "JobA"
    }

    async fn process(&self, _: Self::State) -> ProcessResult {
        eprintln!("Working on {}", self.num);
        time::sleep(Duration::from_secs(5)).await;
        eprintln!("Completed {}", self.num);
        Ok(Status::Completed)
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<()> {
    // Connect to a database called "muffin_testing".
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let database = client.database("muffin_testing");

    // The state is passed to each "process" method on an instance of Job.
    let state = JobState {};
    let config = Config::builder()
        .database(database)
        .polling_interval(1)
        .build()?;
    let muffin = Muffin::with_config(config, Arc::new(state)).await?;

    // Clear jobs from the collection to make the example more predictable
    muffin.clear_all().await?;

    // Register jobs that should be processed.
    muffin.register::<JobA>().await;

    // Generate new jobs forever
    let muffin_gen = muffin.clone();
    tokio::spawn(async move {
        let mut counter = 0;

        loop {
            let _ = muffin_gen.push(JobA { num: counter }).await;
            eprintln!("Inserted a new job with number {}!", counter);
            counter += 1;

            tokio::select! {
                // Sleep for a few seconds to give other tasks a chance to run
                _ = time::sleep(Duration::from_secs(3)) => {},
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("Generator: Received ctrl+c! Breaking out of the loop.");
                    break;
                }
            };
        }
    });

    tokio::select! {
        _ = muffin.run() => {},
        _ = tokio::signal::ctrl_c() => {
            eprintln!("Processor: Received ctrl+c!");
        }
    };

    // Need to wait for all in-flight jobs to finish processing.
    eprintln!("Waiting for all jobs to finish processing...");
    muffin.shutdown().await;

    Ok(())
}
