use std::borrow::Cow;

use chrono::{Duration, Utc};
use futures::future;
use futures::Future;
use futures::FutureExt;
use futures::StreamExt;
use mongodb::{
    bson::{self, bson, doc, oid::ObjectId, Document},
    options::{ChangeStreamOptions, FindOneAndUpdateOptions, IndexOptions},
    results::InsertOneResult,
    IndexModel,
};
use serde::Serialize;

use crate::types::LockedJob;
use crate::Job;
use crate::{
    types::{Error, JobPersistedStatus, Result, Schedule},
    Config,
};

#[derive(Serialize)]
struct NewJob<Job> {
    r#type: &'static str,
    status: JobPersistedStatus,
    status_last_updated_at: bson::DateTime,
    process_at: bson::DateTime,
    created_at: bson::DateTime,
    attempt_count: u16,
    priority: i16,
    #[serde(skip_serializing_if = "Option::is_none")]
    unique_key: Option<Cow<'static, str>>,
    payload: Job,
}

pub async fn create_indexes(config: &Config) -> Result<()> {
    let collection = config.db.collection::<Document>(&config.collection_name);
    let unique_expression = doc! {
        "unique_key": {
            "$exists": true,
        },

        "status": {
            "$in": [
                JobPersistedStatus::Waiting.to_string(),
                JobPersistedStatus::Processing.to_string(),
                JobPersistedStatus::Errored.to_string()
            ]
        }
    };
    let indexes = [
        IndexModel::builder()
            .keys(doc! { "state": 1 })
            .options(IndexOptions::builder().name("state".to_string()).build())
            .build(),
        IndexModel::builder()
            .keys(doc! { "unique_key": 1 })
            .options(
                IndexOptions::builder()
                    .name("unique_key".to_string())
                    .unique(true)
                    .partial_filter_expression(unique_expression)
                    .build(),
            )
            .build(),
    ];
    collection.create_indexes(indexes, None).await?;
    Ok(())
}

pub async fn push<J>(config: &Config, job: J) -> Result<ObjectId>
where
    J: Job + Serialize,
{
    let process_at = Utc::now();
    push_with_schedule(config, job, Schedule::At(process_at)).await
}

pub async fn push_with_schedule<J>(config: &Config, job: J, schedule: Schedule) -> Result<ObjectId>
where
    J: Job + Serialize,
{
    let collection = config.db.collection::<Document>(&config.collection_name);
    let now = now();
    let process_at = schedule_to_datetime(&schedule)?;

    if job.timeout().num_seconds() < 0 {
        return Err(Error::Time("Negative Duration!"));
    }

    let internal_job = NewJob {
        r#type: J::id(),
        status: JobPersistedStatus::Waiting,
        status_last_updated_at: now,
        process_at: process_at.into(),
        created_at: now,
        attempt_count: 0,
        priority: job.priority(),
        unique_key: job.unique_key(),
        payload: job,
    };

    let doc = bson::to_document(&internal_job)?;
    let InsertOneResult { inserted_id, .. } = collection.insert_one(doc, None).await?;

    Ok(inserted_id.as_object_id().unwrap())
}

pub async fn find_and_lock_job(config: &Config, job_names: &[&str]) -> Result<Option<LockedJob>> {
    let collection = config.db.collection::<LockedJob>(&config.collection_name);
    let now = now();

    let filter = doc! {
        "type": {
            "$in": bson!(job_names)
        },

        "status": {
            "$in": [JobPersistedStatus::Waiting.to_string(), JobPersistedStatus::Errored.to_string()]
        },

        "process_at": {
            "$lte": now
        }
    };

    let update = doc! {
        "$set": {
            "status": JobPersistedStatus::Processing.to_string(),
            "status_last_updated": now
        },

        "$inc": {
            "attempt_count": 1
        }
    };

    let sort = doc! {
        "priority": 1
    };

    Ok(collection
        .find_one_and_update(
            filter,
            update,
            FindOneAndUpdateOptions::builder().sort(sort).build(),
        )
        .await?)
}

pub async fn set_job_completed(config: &Config, job: &LockedJob) -> Result<()> {
    let collection = config.db.collection::<Document>(&config.collection_name);
    let now = now();

    let filter = doc! {
        "_id": job._id
    };

    let update = doc! {
        "$set": {
            "status": JobPersistedStatus::Completed.to_string(),
            "status_last_updated": now
        }
    };

    collection.update_one(filter, update, None).await?;

    Ok(())
}

pub async fn delete_job(config: &Config, job: &LockedJob) -> Result<()> {
    let collection = config.db.collection::<Document>(&config.collection_name);

    let filter = doc! {
        "_id": job._id
    };

    collection.delete_one(filter, None).await?;

    Ok(())
}

pub async fn set_job_errorred_or_failed<S>(
    config: &Config,
    job: &dyn Job<State = S>,
    locked_job: &LockedJob,
    error: &str,
) -> Result<()>
where
    S: Send + Sync + 'static,
{
    let status = if locked_job.attempt_count + 1 >= job.max_retries() {
        JobPersistedStatus::Failed
    } else {
        JobPersistedStatus::Errored
    };

    set_job_status_with_error(config, job, locked_job, status, error).await
}

pub async fn set_job_errorred_or_failed_obj<S>(
    config: &Config,
    job: &dyn Job<State = S>,
    locked_job: &LockedJob,
    error: &(dyn std::error::Error + Send + Sync),
) -> Result<()>
where
    S: Send + Sync + 'static,
{
    set_job_errorred_or_failed(config, job, locked_job, &Cow::Owned(error.to_string())).await
}

pub async fn set_job_failed<S>(
    config: &Config,
    job: &dyn Job<State = S>,
    locked_job: &LockedJob,
    error: &str,
) -> Result<()>
where
    S: Send + Sync + 'static,
{
    set_job_status_with_error(config, job, locked_job, JobPersistedStatus::Failed, error).await
}

pub async fn retry_job_in(
    config: &Config,
    locked_job: &LockedJob,
    retry_in: &Duration,
) -> Result<()> {
    let collection = config.db.collection::<Document>(&config.collection_name);
    let now = now();
    let process_at = Utc::now()
        .checked_add_signed(*retry_in)
        .ok_or(Error::Time("time overflow detected"))?;

    let filter = doc! {
        "_id": locked_job._id
    };

    let update = doc! {
        "$set": {
            "status": JobPersistedStatus::Waiting.to_string(),
            "status_last_updated": now,
            "process_at": process_at
        }
    };

    collection.update_one(filter, update, None).await?;

    Ok(())
}

pub async fn create_change_stream(config: &Config) -> Result<impl Future<Output = ()>> {
    let collection = config.db.collection::<Document>(&config.collection_name);

    #[rustfmt::skip]
    let pipeline = [
        doc! {
            "$match": {
                "operationType": "insert"
            }
        },
        
        doc! { 
            "$project": {
                "_id": 1,
                "operationType": 1,
                "ns": 1,
            }
        },
    ];

    let change_stream = collection
        .watch(
            pipeline,
            ChangeStreamOptions::builder().batch_size(1.into()).build(),
        )
        .await?;

    Ok(change_stream.into_future().then(|_| future::ready(())))
}

pub async fn set_job_failed_to_deserialize(
    config: &Config,
    locked_job: &LockedJob,
    error: &str,
) -> Result<()> {
    let collection = config.db.collection::<Document>(&config.collection_name);
    let now = now();

    let filter = doc! {
        "_id": locked_job._id
    };

    let update = doc! {
        "$set": {
            "status": JobPersistedStatus::Failed.to_string(),
            "status_last_updated": now,
        },

        "$push": {
            "errors": {
                "created_at": now,
                "message": error,
            }
        }
    };

    collection.update_one(filter, update, None).await?;

    Ok(())
}

pub async fn clear_collection(config: &Config) -> Result<()> {
    let collection = config.db.collection::<Document>(&config.collection_name);
    collection.delete_many(doc! {}, None).await?;
    Ok(())
}

pub async fn clear_collection_with_status(
    config: &Config,
    status: JobPersistedStatus,
) -> Result<()> {
    let collection = config.db.collection::<Document>(&config.collection_name);

    let filter = doc! {
        "status": status.to_string(),
    };

    collection.delete_many(filter, None).await?;
    Ok(())
}

pub async fn document_count(config: &Config, job_names: &[&str]) -> Result<u64> {
    let collection = config.db.collection::<Document>(&config.collection_name);

    let filter = doc! {
        "type": {
            "$in": bson!(job_names)
        },

        "status": {
            "$in": [JobPersistedStatus::Waiting.to_string(), JobPersistedStatus::Errored.to_string()]
        },

        "process_at": {
            "$lte": now()
        }
    };

    Ok(collection.count_documents(filter, None).await?)
}

async fn set_job_status_with_error<S>(
    config: &Config,
    job: &dyn Job<State = S>,
    locked_job: &LockedJob,
    status: JobPersistedStatus,
    error: &str,
) -> Result<()>
where
    S: Send + Sync + 'static,
{
    let collection = config.db.collection::<Document>(&config.collection_name);
    let now = now();

    let filter = doc! {
        "_id": locked_job._id
    };

    let update = doc! {
        "$set": {
            "status": status.to_string(),
            "status_last_updated": now,
            "process_at": process_at(job, locked_job)?,
        },

        "$push": {
            "errors": {
                "created_at": now,
                "message": error,
            }
        }
    };

    collection.update_one(filter, update, None).await?;

    Ok(())
}

fn now() -> bson::DateTime {
    Utc::now().into()
}

fn schedule_to_datetime(schedule: &Schedule) -> Result<chrono::DateTime<Utc>> {
    match schedule {
        Schedule::At(datetime) => Ok(*datetime),
        Schedule::In(duration) => Utc::now()
            .checked_add_signed(*duration)
            .ok_or(Error::Time("time overflow detected")),
    }
}

fn process_at<S>(job: &dyn Job<State = S>, locked_job: &LockedJob) -> Result<chrono::DateTime<Utc>>
where
    S: Send + Sync + 'static,
{
    let backoff = job.backoff(locked_job.attempt_count + 1);
    Utc::now()
        .checked_add_signed(backoff)
        .ok_or(Error::Time("time overflow detected"))
}
