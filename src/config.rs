use std::borrow::Cow;

use mongodb::Database;

use crate::types::{Error, Result};

/// Use a [`ConfigBuilder`] to create an instance of [`Config`].
///
/// # Example
///
///```no_run
///# use mongodb::Client;
///# use std::sync::Arc;
///# use muffin::{Config, Muffin};
///# struct State {}
///# #[tokio::main]
///# async fn main() -> muffin::Result<()> {
///# let client = Client::with_uri_str("mongodb://localhost:27017").await?;
///# let database = client.database("muffin_testing");
///# let state = State {};
///let config = Config::builder()
///    .database(database)
///    .collection_name("muffin_jobs")
///    .build()?;
///let mut muffin = Muffin::with_config(config, Arc::new(state)).await?;
///# Ok(())
///# }
///```
#[non_exhaustive]
pub struct Config {
    pub db: Database,
    pub collection_name: Cow<'static, str>,
    pub keep_completed: bool,
    pub create_indexes: bool,
    pub polling_interval: u64,
    pub concurrency: usize,
}

/// Creates a Config using the builder interface.
///
/// # Example
///
///```no_run
///# use mongodb::Client;
///# use std::sync::Arc;
///# use muffin::{Config, Muffin};
///# struct State {}
///# #[tokio::main]
///# async fn main() -> muffin::Result<()> {
///# let client = Client::with_uri_str("mongodb://localhost:27017").await?;
///# let database = client.database("muffin_testing");
///# let state = State {};
///let config = Config::builder()
///    .database(database)
///    .collection_name("muffin_jobs")
///    .build()?;
///let mut muffin = Muffin::with_config(config, Arc::new(state)).await?;
///# Ok(())
///# }
///```
#[derive(Default)]
#[non_exhaustive]
pub struct ConfigBuilder {
    db: Option<Database>,
    collection_name: Option<Cow<'static, str>>,
    keep_completed: Option<bool>,
    create_indexes: Option<bool>,
    polling_interval: Option<u64>,
    concurrency: Option<usize>,
}

impl ConfigBuilder {
    /// Sets the database name to use. Required.
    pub fn database<D>(mut self, db: D) -> Self
    where
        D: Into<Database>,
    {
        self.db = Some(db.into());
        self
    }

    /// Sets the collection name where jobs should be persisted. If not
    /// provided, it defaults to "muffin_jobs".
    pub fn collection_name<N>(mut self, collection_name: N) -> Self
    where
        N: Into<Cow<'static, str>>,
    {
        self.collection_name = Some(collection_name.into());
        self
    }

    /// If true, jobs which were successfully completed are automatically
    /// deleted. Defaults to true.
    pub fn keep_completed(mut self, keep_completed: bool) -> Self {
        self.keep_completed = Some(keep_completed);
        self
    }

    /// If true, the library will automatically create MongoDB indexes in
    /// the specified collection to make queries for jobs more efficient, and also to
    /// enforce jobs with unique keys.
    pub fn create_indexes(mut self, create_indexes: bool) -> Self {
        self.create_indexes = Some(create_indexes);
        self
    }

    /// Sets the polling interval for [`Muffin::run()`] and [`Muffin::run_with_watch()`].
    ///
    /// [`Muffin::run()`]: crate::Muffin::run()
    /// [`Muffin::run_with_watch()`]: crate::Muffin::run_with_watch()
    pub fn polling_interval(mut self, polling_interval: u64) -> Self {
        self.polling_interval = Some(polling_interval);
        self
    }

    /// Sets the number of jobs to run at the same time for [`Muffin::run()`] and
    /// [`Muffin::run_with_watch()`].
    ///
    /// [`Muffin::run()`]: crate::Muffin::run()
    /// [`Muffin::run_with_watch()`]: crate::Muffin::run_with_watch()
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    /// Builds the config.
    pub fn build(mut self) -> Result<Config> {
        Ok(Config {
            db: {
                if let Some(db) = self.db.take() {
                    db
                } else {
                    return Err(Error::Config("You need to set database!"));
                }
            },
            collection_name: self
                .collection_name
                .take()
                .unwrap_or_else(|| "muffin_jobs".into()),
            keep_completed: self.keep_completed.unwrap_or(false),
            create_indexes: self.create_indexes.unwrap_or(true),
            polling_interval: self.polling_interval.unwrap_or(10),
            concurrency: self.concurrency.unwrap_or(8),
        })
    }
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}
