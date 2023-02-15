mod utils;

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use async_trait::async_trait;
    use expecting::expect_eq;
    use expecting::expect_none;
    use mongodb::bson::{doc, Document};
    use mongodb::Collection;
    use muffin::{Job, Muffin, ProcessResult, Status};
    use serde::{Deserialize, Serialize};

    use crate::utils;
    use crate::utils::JobState;

    #[tokio::test]
    async fn lowest_first() -> Result<(), Box<dyn Error>> {
        #[derive(Serialize, Deserialize)]
        struct MyJob {
            prio: i16,
        }

        #[async_trait]
        impl Job for MyJob {
            type State = Arc<JobState>;

            fn id() -> &'static str
            where
                Self: Sized,
            {
                "MyJob"
            }

            fn priority(&self) -> i16 {
                self.prio
            }

            async fn process(&self, _: Self::State) -> ProcessResult {
                Ok(Status::Completed)
            }
        }

        utils::with_muffin_instance(
            |col: Collection<Document>, muffin: Muffin<Arc<JobState>>| async move {
                muffin.register::<MyJob>().await;

                muffin.push(MyJob { prio: 42 }).await?;
                let id = muffin.push(MyJob { prio: 2 }).await?;
                muffin.push(MyJob { prio: 100 }).await?;
                muffin.push(MyJob { prio: 57 }).await?;

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 4);

                muffin.process_n(1).await?;

                let filter = doc! {
                    "_id": id
                };
                expect_none!(col.find_one(filter, None).await?);

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 3);

                Ok(())
            },
        )
        .await
    }
}
