mod utils;

#[cfg(test)]
mod tests {
    use std::{error::Error, sync::Arc};

    use async_trait::async_trait;
    use chrono::Duration;
    use chrono::TimeZone;
    use chrono::Utc;
    use expecting::expect;
    use expecting::expect_eq;
    use expecting::expect_ok;
    use expecting::expect_some;
    use mongodb::bson::{doc, Document};
    use mongodb::Collection;
    use muffin::{Job, Muffin, ProcessResult, Schedule, Status};
    use serde::{Deserialize, Serialize};

    use crate::utils;
    use crate::utils::JobState;

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

    #[tokio::test]
    async fn schedule_in() -> Result<(), Box<dyn Error>> {
        utils::with_muffin_instance(
            |col: Collection<Document>, muffin: Muffin<Arc<JobState>>| async move {
                let now = Utc::now();
                let hours = Duration::hours(2);

                let id = muffin.schedule(MyJob {}, Schedule::In(hours)).await?;

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 1);

                let filter = doc! {
                    "_id": id
                };
                let doc = expect_some!(col.find_one(filter, None).await?);

                let process_at = expect_ok!(doc.get_datetime("process_at"));
                let process_at = process_at.to_chrono();

                let before = expect_some!(now.checked_add_signed(hours));
                let before = expect_some!(before.checked_sub_signed(Duration::seconds(2)));

                let after = expect_some!(now.checked_add_signed(hours));
                let after = expect_some!(after.checked_add_signed(Duration::seconds(2)));

                expect!(process_at >= before);
                expect!(process_at <= after);

                Ok(())
            },
        )
        .await
    }

    #[tokio::test]
    async fn schedule_at() -> Result<(), Box<dyn Error>> {
        utils::with_muffin_instance(
            |col: Collection<Document>, muffin: Muffin<Arc<JobState>>| async move {
                let at = Utc.with_ymd_and_hms(2023, 6, 2, 0, 0, 42).unwrap();

                let id = muffin.schedule(MyJob {}, Schedule::At(at)).await?;

                let count = col.count_documents(doc! {}, None).await?;
                expect_eq!(count, 1);

                let filter = doc! {
                    "_id": id
                };
                let doc = expect_some!(col.find_one(filter, None).await?);

                let process_at = expect_ok!(doc.get_datetime("process_at"));
                let process_at = process_at.to_chrono();

                let before = expect_some!(at.checked_sub_signed(Duration::seconds(2)));
                let after = expect_some!(at.checked_add_signed(Duration::seconds(2)));

                expect!(process_at >= before);
                expect!(process_at <= after);

                Ok(())
            },
        )
        .await
    }
}
