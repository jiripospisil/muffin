mod utils;

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::{error::Error, sync::Arc};

    use async_trait::async_trait;
    use expecting::expect;
    use expecting::expect_eq;
    use expecting::expect_err;
    use expecting::expect_ok;
    use mongodb::bson::{doc, Document};
    use mongodb::Collection;
    use muffin::{Job, Muffin, ProcessResult, Status};
    use serde::{Deserialize, Serialize};

    use crate::utils;
    use crate::utils::JobState;

    #[tokio::test]
    async fn unique() -> Result<(), Box<dyn Error>> {
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

            fn unique_key(&self) -> Option<Cow<'static, str>> {
                Some("so much unique".into())
            }

            async fn process(&self, _: Self::State) -> ProcessResult {
                Ok(Status::Completed)
            }
        }

        utils::with_muffin_instance(
            |col: Collection<Document>, muffin: Muffin<Arc<JobState>>| async move {
                muffin.register::<MyJob>().await;

                muffin.push(MyJob {}).await?;

                let err = expect_err!(muffin.push(MyJob {}).await);
                expect!(matches!(err, muffin::Error::Mongo(_)));

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 1);

                muffin.process_n(1).await?;

                expect_ok!(muffin.push(MyJob {}).await);

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 1);

                Ok(())
            },
        )
        .await
    }
}
