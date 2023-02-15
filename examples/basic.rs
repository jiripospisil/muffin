use std::sync::Arc;

use async_trait::async_trait;
use mongodb::{bson::doc, Client};
use muffin::{Job, Muffin, ProcessResult, Result, Status};
use serde::{Deserialize, Serialize};

struct JobState {
    mongo_client: Client,
}

#[derive(Serialize, Deserialize)]
struct JobA {
    name: String,
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

    async fn process(&self, state: Self::State) -> ProcessResult {
        eprintln!("{}: Hello {}", Self::id(), self.name);

        // Use the state. For example for getting the names of existing
        // databases. Errors automatically error the job.
        let databases = state.mongo_client.list_databases(doc! {}, None).await?;
        let database_names = databases
            .iter()
            .map(|spec| spec.name.as_ref())
            .collect::<Vec<_>>();

        eprintln!(
            "{}: There are the following databases available: {}",
            Self::id(),
            database_names.join(", ")
        );

        Ok(Status::Completed)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to a database called "muffin_testing".
    let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let database = client.database("muffin_testing");

    // The state is passed to each "process" method on an instance of Job.
    let state = JobState {
        mongo_client: client,
    };
    let muffin = Muffin::new(database, Arc::new(state)).await?;

    muffin
        .push(JobA {
            name: "Peter".into(),
        })
        .await?;

    muffin
        .push(JobA {
            name: "Lois".into(),
        })
        .await?;

    // Register jobs that should be processed.
    muffin.register::<JobA>().await;

    // Process 2 jobs and wait for them to finish.
    muffin.process_n(2).await?;

    Ok(())
}
