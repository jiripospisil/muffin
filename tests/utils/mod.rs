use std::{error::Error, sync::Arc};

use futures::Future;
use mongodb::{bson::Document, Client, Collection};
use muffin::{Config, Muffin};

// https://github.com/rust-lang/rust/pull/93582
pub trait AsyncFn2<A, B>: Fn(A, B) -> <Self as AsyncFn2<A, B>>::Future {
    type Future: Future<Output = Self::Out>;
    type Out;
}

impl<A, B, Fut, F> AsyncFn2<A, B> for F
where
    F: Fn(A, B) -> Fut,
    Fut: Future,
{
    type Future = Fut;
    type Out = Fut::Output;
}

pub struct JobState {}

pub async fn with_muffin_instance<F>(f: F) -> Result<(), Box<dyn Error>>
where
    F: AsyncFn2<Collection<Document>, Muffin<Arc<JobState>>, Out = Result<(), Box<dyn Error>>>,
{
    let random_num = rand::random::<usize>();
    let col_name = format!("test-collection-{}", random_num);

    let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    let database = client.database("muffin_test");
    let col = database.collection::<Document>(&col_name);

    let config = Config::builder()
        .database(database.clone())
        .collection_name(col_name.clone())
        .build()?;

    let state = JobState {};
    let muffin = Muffin::with_config(config, Arc::new(state)).await?;

    let res = f(col.clone(), muffin.clone()).await;

    col.drop(None).await?;

    res
}
