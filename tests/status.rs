mod utils;

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use async_trait::async_trait;
    use expecting::expect_eq;
    use expecting::expect_ok;
    use expecting::expect_some;
    use mongodb::bson::{doc, Document};
    use mongodb::Collection;
    use muffin::JobPersistedStatus;
    use muffin::{Job, Muffin, ProcessResult, Status};
    use serde::{Deserialize, Serialize};

    use crate::utils;
    use crate::utils::JobState;

    #[tokio::test]
    async fn waiting() -> Result<(), Box<dyn Error>> {
        #[derive(Serialize, Deserialize)]
        struct MyJob {}

        #[async_trait]
        impl Job for MyJob {
            type State = Arc<JobState>;

            fn id() -> &'static str
            where
                Self: Sized,
            {
                "MyJob"
            }

            async fn process(&self, _: Self::State) -> ProcessResult {
                Ok(Status::Completed)
            }
        }

        utils::with_muffin_instance(
            |col: Collection<Document>, muffin: Muffin<Arc<JobState>>| async move {
                muffin.register::<MyJob>().await;

                let id = muffin.push(MyJob {}).await?;

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 1);

                let filter = doc! {
                    "_id": id
                };
                let doc = expect_some!(col.find_one(filter, None).await?);

                let status = expect_ok!(doc.get_str("status"));
                expect_eq!(status, JobPersistedStatus::Waiting.to_string());

                Ok(())
            },
        )
        .await
    }

    #[tokio::test]
    async fn errored() -> Result<(), Box<dyn Error>> {
        #[derive(Serialize, Deserialize)]
        struct MyJob {}

        #[async_trait]
        impl Job for MyJob {
            type State = Arc<JobState>;

            fn id() -> &'static str
            where
                Self: Sized,
            {
                "MyJob"
            }

            async fn process(&self, _: Self::State) -> ProcessResult {
                Ok(Status::Error("error!".into()))
            }
        }

        utils::with_muffin_instance(
            |col: Collection<Document>, muffin: Muffin<Arc<JobState>>| async move {
                muffin.register::<MyJob>().await;

                let id = muffin.push(MyJob {}).await?;

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 1);

                muffin.process_all(10).await?;

                let filter = doc! {
                    "_id": id
                };
                let doc = expect_some!(col.find_one(filter, None).await?);

                let status = expect_ok!(doc.get_str("status"));
                expect_eq!(status, JobPersistedStatus::Errored.to_string());

                let attempt_count = expect_ok!(doc.get_i32("attempt_count"));
                expect_eq!(attempt_count, 1);

                let errors = expect_ok!(doc.get_array("errors"));
                let error = expect_some!(errors[0].as_document());
                let error = expect_ok!(error.get_str("message"));
                expect_eq!(error, "error!");

                Ok(())
            },
        )
        .await
    }

    #[tokio::test]
    async fn failed() -> Result<(), Box<dyn Error>> {
        #[derive(Serialize, Deserialize)]
        struct MyJob {}

        #[async_trait]
        impl Job for MyJob {
            type State = Arc<JobState>;

            fn id() -> &'static str
            where
                Self: Sized,
            {
                "MyJob"
            }

            async fn process(&self, _: Self::State) -> ProcessResult {
                Ok(Status::Fail("fail!".into()))
            }
        }

        utils::with_muffin_instance(
            |col: Collection<Document>, muffin: Muffin<Arc<JobState>>| async move {
                muffin.register::<MyJob>().await;

                let id = muffin.push(MyJob {}).await?;

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 1);

                muffin.process_all(10).await?;

                let filter = doc! {
                    "_id": id
                };
                let doc = expect_some!(col.find_one(filter, None).await?);

                let status = expect_ok!(doc.get_str("status"));
                expect_eq!(status, JobPersistedStatus::Failed.to_string());

                let attempt_count = expect_ok!(doc.get_i32("attempt_count"));
                expect_eq!(attempt_count, 1);

                let errors = expect_ok!(doc.get_array("errors"));
                let error = expect_some!(errors[0].as_document());
                let error = expect_ok!(error.get_str("message"));
                expect_eq!(error, "fail!");

                Ok(())
            },
        )
        .await
    }

    #[tokio::test]
    async fn failed_because_of_errors() -> Result<(), Box<dyn Error>> {
        #[derive(Serialize, Deserialize)]
        struct MyJob {}

        #[async_trait]
        impl Job for MyJob {
            type State = Arc<JobState>;

            fn id() -> &'static str
            where
                Self: Sized,
            {
                "MyJob"
            }

            fn max_retries(&self) -> u16 {
                1
            }

            async fn process(&self, _: Self::State) -> ProcessResult {
                Ok(Status::Error("error!".into()))
            }
        }

        utils::with_muffin_instance(
            |col: Collection<Document>, muffin: Muffin<Arc<JobState>>| async move {
                muffin.register::<MyJob>().await;

                let id = muffin.push(MyJob {}).await?;

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 1);

                muffin.process_all(10).await?;

                let filter = doc! {
                    "_id": id
                };
                let doc = expect_some!(col.find_one(filter, None).await?);

                let status = expect_ok!(doc.get_str("status"));
                expect_eq!(status, JobPersistedStatus::Failed.to_string());

                let attempt_count = expect_ok!(doc.get_i32("attempt_count"));
                expect_eq!(attempt_count, 1);

                let errors = expect_ok!(doc.get_array("errors"));
                let error = expect_some!(errors[0].as_document());
                let error = expect_ok!(error.get_str("message"));
                expect_eq!(error, "error!");

                Ok(())
            },
        )
        .await
    }
}
