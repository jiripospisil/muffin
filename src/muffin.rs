use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use mongodb::{bson::oid::ObjectId, Database};
use parking_lot::{Mutex, RwLock};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use smallvec::SmallVec;
use tokio::{sync::Semaphore, task::JoinHandle, time};

use crate::{
    db,
    types::{DeserializerFn, LockedJob, ProcessJobsResult, Result},
    Config, Job, JobPersistedStatus, Schedule, Status,
};

struct MuffinInner<State>
where
    State: Clone + Send + Sync + 'static,
{
    config: Config,
    state: State,
    deserializers: RwLock<HashMap<&'static str, DeserializerFn<State>>>,
    handle_set: Mutex<VecDeque<JoinHandle<ProcessJobsResult>>>,
    concurrency_limit: Arc<Semaphore>,
}

/// Muffin is the primary struct the users of the library will interact with.
/// It's constructed with a [`configuration `](crate::Config) (or just a
/// database connection) and a state. The state provides a way of easily
/// passing variables into individual jobs. Since the jobs can be moved between
/// threads, the state must be safe to move between threads as well.
///
/// You can find usage examples in the [`repository`](https://github.com/jiripospisil/muffin/tree/master/examples).
#[derive(Clone)]
pub struct Muffin<State>
where
    State: Clone + Send + Sync + 'static,
{
    inner: Arc<MuffinInner<State>>,
}

impl<'a, State> Muffin<State>
where
    State: Clone + Send + Sync + 'static,
{
    /// Creates a new instance of Muffin with the given MongoDB database
    /// connection and state. The state is what allows you to easily pass instances of
    /// any objects to your jobs.
    pub async fn new(database: Database, state: State) -> Result<Self> {
        let config = Config::builder().database(database).build()?;
        Self::with_config(config, state).await
    }

    /// Creates a new instance of Muffin with the given config and state. The
    /// state is what allows you to easily pass instances of any objects to your jobs.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = Config::builder()
    ///     .database(database)
    ///     .collection_name("muffin_jobs")
    ///     .keep_completed(true)
    ///     .build()?;
    ///
    /// let mut muffin = Muffin::with_config(config, Arc::new(state)).await?;
    /// ```
    ///
    /// Take a look at the [`Config`]'s module documentation for more information.
    pub async fn with_config(config: Config, state: State) -> Result<Self> {
        let concurrency = config.concurrency;

        let core = MuffinInner {
            config,
            state,
            deserializers: Default::default(),
            handle_set: Default::default(),
            concurrency_limit: Arc::new(Semaphore::new(concurrency)),
        };

        if core.config.create_indexes {
            db::create_indexes(&core.config).await?;
        }

        Ok(Muffin {
            inner: Arc::new(core),
        })
    }

    /// Creates a new job and persists it in the database. Jobs are processed ordered by
    /// their priority. The lower the priority, the sooner the job gets picked up.
    ///    
    /// # Example
    ///
    /// ```ignore
    /// muffin
    ///     .push(JobA {
    ///         name: "Peter".into(),
    ///     })
    ///     .await?;
    ///````
    pub async fn push<J>(&self, job: J) -> Result<ObjectId>
    where
        J: Job<State = State> + Serialize,
    {
        db::push(&self.inner.config, job).await
    }

    /// Creates a new job and persists it in the database. Jobs are processed
    /// ordered by their priority. The lower the priority, the sooner the job
    /// gets picked up.
    ///
    /// This method is the same as `push` but allows to delay processing of the job
    /// to a later date.
    ///    
    /// # Example
    ///
    /// ```ignore
    /// muffin
    ///     .schedule(
    ///         JobA {
    ///             name: "Peter".into(),
    ///         },
    ///         Schedule::At(Utc.with_ymd_and_hms(2023, 6, 2, 0, 0, 42).unwrap()),
    ///     )
    ///     .await?;
    ///
    /// muffin
    ///     .schedule(
    ///         JobA {
    ///             name: "Lois".into(),
    ///         },
    ///         Schedule::In(Duration::hours(2)),
    ///     )
    ///     .await?;
    /// ```
    pub async fn schedule<J>(&self, job: J, schedule: Schedule) -> Result<ObjectId>
    where
        J: Job<State = State> + Serialize,
    {
        db::push_with_schedule(&self.inner.config, job, schedule).await
    }

    /// Registers a given job type for processing. Job types which are pushed/
    /// scheduled are still persisted but are not picked up for processing.
    pub async fn register<J>(&self)
    where
        J: Job<State = State> + DeserializeOwned,
    {
        let mut deserializers = self.inner.deserializers.write();

        deserializers.insert(
            J::id(),
            Box::new(|serialized_job| deserialize::<State, J>(serialized_job)),
        );
    }

    /// Processes incoming jobs indefinitely.
    ///
    /// It creates an infinite loop which polls for new jobs and processes
    /// them. You can configure the polling interval with [`polling_interval`]
    /// and [`concurrency`] on an instance of [`ConfigBuilder`]. Defaults to 10 seconds
    /// and 8 jobs respectively.
    ///
    /// [`polling_interval`]: crate::ConfigBuilder::polling_interval()
    /// [`concurrency`]: crate::ConfigBuilder::concurrency()
    /// [`ConfigBuilder`]: crate::config::ConfigBuilder
    pub async fn run(&self) -> Result<()> {
        self.run_(false).await
    }

    /// Processes incoming jobs indefinitely.
    ///
    /// This is almost exactly the same as [`Muffin::run`] except in addition
    /// to polling, it uses MongoDB's [Change Streams] to immediately start
    /// working on new jobs.
    ///
    /// **Note**: Change streams are only available on replica sets and sharded
    /// clusters. Calling this method on a standalone mongod deployment will return
    /// an error.
    ///
    /// [Change Streams]: https://www.mongodb.com/docs/manual/changeStreams/
    pub async fn run_with_watch(&self) -> Result<()> {
        self.run_(true).await
    }

    /// Processes the specified number of jobs (if any) and waits for them
    /// to finish. Use only if you want to do your own polling, use [`run`] or
    /// [`run_with_watch`] otherwise.
    ///    
    /// [`run`]: crate::Muffin::run
    /// [`run_with_watch`]: crate::Muffin::run_with_watch
    ///
    /// Returns the number of processed jobs.
    pub async fn process_n(&self, n: usize) -> Result<usize> {
        debug_assert!(n > 0, "n must be greater than zero");

        for _ in 0..n {
            let permit = self.inner.concurrency_limit.clone().acquire_owned().await?;

            let muffin = self.clone();
            let join_handle = tokio::spawn(async move {
                let result = find_and_process_job(muffin).await;
                drop(permit);
                result
            });

            self.inner.handle_set.lock().push_back(join_handle);
        }

        let mut processed = 0;
        let mut count = 0;

        while count < n {
            let handle = self.inner.handle_set.lock().pop_back();

            if let Some(handle) = handle {
                if let Ok(res) = handle.await {
                    if !matches!(res, ProcessJobsResult::NotFound) {
                        processed += 1;
                    }
                }
            }

            count += 1;
        }

        Ok(processed)
    }

    /// Processes all available jobs. Use only if you want to do your own
    /// polling, use [`run`] or [`run_with_watch`] otherwise.
    ///
    /// [`run`]: crate::Muffin::run
    /// [`run_with_watch`]: crate::Muffin::run_with_watch
    ///
    /// Returns the number of processed jobs.
    pub async fn process_all(&self, batch_size: usize) -> Result<usize> {
        let mut count = 0;

        loop {
            match self.process_n(batch_size).await {
                Ok(num) => match num {
                    0 => return Ok(count),
                    num => count += num,
                },
                Err(err) => return Err(err),
            }
        }
    }

    /// Waits for all current jobs to finish.
    ///
    /// This must be called before dropping the Muffin instance, otherwise jobs
    /// might get stuck in the middle of processing.
    pub async fn shutdown(&self) {
        loop {
            let handle = self.inner.handle_set.lock().pop_back();

            if let Some(handle) = handle {
                _ = handle.await;
            } else {
                break;
            }
        }
    }

    /// Deletes *all* jobs regardless of their status.
    pub async fn clear_all(&self) -> Result<()> {
        db::clear_collection(&self.inner.config).await
    }

    /// Deletes jobs with the given status.
    pub async fn clear_with_status(&self, status: JobPersistedStatus) -> Result<()> {
        db::clear_collection_with_status(&self.inner.config, status).await
    }

    async fn run_(&self, with_watch: bool) -> Result<()> {
        loop {
            {
                let mut handle_set = self.inner.handle_set.lock();
                handle_set.retain(|handle| !handle.is_finished());
            }

            let count = db::document_count(&self.inner.config, &self.job_names()).await?;

            for _ in 0..count {
                let permit = self.inner.concurrency_limit.clone().acquire_owned().await?;

                let muffin = self.clone();
                let join_handle = tokio::spawn(async move {
                    let result = find_and_process_job(muffin).await;
                    drop(permit);
                    result
                });

                self.inner.handle_set.lock().push_back(join_handle);
            }

            if with_watch {
                let change_stream = db::create_change_stream(&self.inner.config).await?;
                let timeout = time::sleep(Duration::from_secs(self.inner.config.polling_interval));

                tokio::select! {
                    _ = change_stream => {},
                    _ = timeout => {}
                };
            } else {
                time::sleep(Duration::from_secs(self.inner.config.polling_interval)).await;
            }
        }
    }

    fn job_names(&self) -> SmallVec<[&str; 64]> {
        let mut vec = SmallVec::<[&str; 64]>::new();
        vec.extend(self.inner.deserializers.read().keys().cloned());
        vec
    }
}

async fn find_and_process_job<S>(muffin: Muffin<S>) -> ProcessJobsResult
where
    S: Clone + Send + Sync + 'static,
{
    let job = db::find_and_lock_job(&muffin.inner.config, &muffin.job_names()).await;

    match job {
        Ok(job) => match job {
            Some(job) => process_job(muffin.clone(), job).await,
            None => ProcessJobsResult::NotFound,
        },
        Err(err) => ProcessJobsResult::Error(err),
    }
}

async fn process_job<S>(muffin: Muffin<S>, locked_job: LockedJob) -> ProcessJobsResult
where
    S: Clone + Send + Sync + 'static,
{
    let job = {
        let deserializers = &muffin.inner.deserializers.read();
        let deserializer = deserializers.get(locked_job.r#type.as_str()).unwrap();

        deserializer(locked_job.payload.clone())
    };

    match job {
        Ok(job) => {
            let status = tokio::select! {
                status = process(muffin.inner.state.clone(), job.as_ref()) => {
                    status
                },
                _ = time::sleep(job.timeout().to_std().expect("Negative Duration!")) => {
                    Status::Error("Timed out".into())
                }
            };

            let config = &muffin.inner.config;

            let res = match &status {
                Status::Completed => {
                    if config.keep_completed {
                        db::set_job_completed(config, &locked_job).await
                    } else {
                        db::delete_job(config, &locked_job).await
                    }
                }
                Status::Error(err) => {
                    db::set_job_errorred_or_failed(config, job.as_ref(), &locked_job, err).await
                }
                Status::ErrorObj(err) => {
                    db::set_job_errorred_or_failed_obj(
                        config,
                        job.as_ref(),
                        &locked_job,
                        err.as_ref(),
                    )
                    .await
                }
                Status::Fail(err) => {
                    db::set_job_failed(config, job.as_ref(), &locked_job, err).await
                }
                Status::RetryIn(duration) => db::retry_job_in(config, &locked_job, duration).await,
            };

            if let Err(err) = res {
                return ProcessJobsResult::Error(err);
            }

            ProcessJobsResult::Processed(locked_job, status)
        }
        Err(err) => {
            _ = db::set_job_failed_to_deserialize(
                &muffin.inner.config,
                &locked_job,
                &err.to_string(),
            )
            .await;
            ProcessJobsResult::Error(err)
        }
    }
}

fn deserialize<S, J>(serialized_job: Value) -> Result<Box<dyn Job<State = S>>>
where
    J: Job<State = S> + DeserializeOwned,
{
    let job: J = serde_json::from_value(serialized_job)?;
    Ok(Box::new(job))
}

async fn process<State>(state: State, job: &dyn Job<State = State>) -> Status
where
    State: Send + Sync + 'static,
{
    match job.process(state).await {
        Ok(status) => status,
        Err(err) => Status::ErrorObj(err),
    }
}
