//! Provides implementations of the most common backoff strategies. You can
//! also implement your own.
//!
//! ```no_run
//! # use std::sync::Arc;
//! # use async_trait::async_trait;
//! # use muffin::{backoff, Job, ProcessResult};
//! #
//! # struct JobA {}
//! # struct JobState {}
//! use chrono::Duration;
//!
//! # #[async_trait]
//! impl Job for JobA {
//!     # type State = Arc<JobState>;
//!     # fn id() -> &'static str
//!     # where
//!     #     Self: Sized,
//!     # {
//!     #     // A unique identifier of the job
//!     #     "JobA"
//!     # }
//!     # async fn process(&self, state: Self::State) -> ProcessResult {
//!     #     todo!();
//!     # }
//!     fn backoff(&self, retries: u16) -> Duration {
//!         backoff::exponential(Duration::minutes(2), 3, retries)
//!     }
//!     // ...
//! }
//! ```

use chrono::Duration;

/// Exponential backoff strategy capped at 24 hours. This is the default
/// strategy with the number of retries passed as the exponent.
///
/// ```
/// use chrono::Duration;
/// use muffin::backoff::exponential;
///
/// // The second retry will start in 10 minutes, the third in 20 etc.
/// assert_eq!(10, exponential(Duration::minutes(5), 2, 1).num_minutes());
/// assert_eq!(20, exponential(Duration::minutes(5), 2, 2).num_minutes());
/// assert_eq!(40, exponential(Duration::minutes(5), 2, 3).num_minutes());
/// assert_eq!(80, exponential(Duration::minutes(5), 2, 4).num_minutes());
/// ```
pub fn exponential(delay: Duration, base: u16, retries: u16) -> Duration {
    let duration = Duration::minutes(delay.num_minutes() * base.pow(retries as u32) as i64);
    Duration::days(1).min(duration)
}

/// Linear backoff strategy capped at 24 hours.
///
/// ```
/// use chrono::Duration;
/// use muffin::backoff::linear;
///
/// // The second retry will start in 4 minutes, the third in 8 etc.
/// assert_eq!(4,  linear(Duration::minutes(4), 1).num_minutes());
/// assert_eq!(8,  linear(Duration::minutes(4), 2).num_minutes());
/// assert_eq!(12, linear(Duration::minutes(4), 3).num_minutes());
/// assert_eq!(16, linear(Duration::minutes(4), 4).num_minutes());
/// ````
pub fn linear(delay: Duration, retries: u16) -> Duration {
    let duration = Duration::minutes(delay.num_minutes() * retries as i64);
    Duration::days(1).min(duration)
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::{exponential, linear};

    #[test]
    fn exponential_fn() {
        assert_eq!(2, exponential(Duration::minutes(1), 2, 1).num_minutes());
        assert_eq!(4, exponential(Duration::minutes(1), 2, 2).num_minutes());
        assert_eq!(8, exponential(Duration::minutes(1), 2, 3).num_minutes());
        assert_eq!(16, exponential(Duration::minutes(1), 2, 4).num_minutes());

        assert_eq!(10, exponential(Duration::minutes(5), 2, 1).num_minutes());
        assert_eq!(20, exponential(Duration::minutes(5), 2, 2).num_minutes());
        assert_eq!(40, exponential(Duration::minutes(5), 2, 3).num_minutes());
        assert_eq!(80, exponential(Duration::minutes(5), 2, 4).num_minutes());

        assert_eq!(6, exponential(Duration::minutes(2), 3, 1).num_minutes());
        assert_eq!(18, exponential(Duration::minutes(2), 3, 2).num_minutes());
        assert_eq!(54, exponential(Duration::minutes(2), 3, 3).num_minutes());
        assert_eq!(162, exponential(Duration::minutes(2), 3, 4).num_minutes());
    }

    #[test]
    fn linear_fn() {
        assert_eq!(4, linear(Duration::minutes(4), 1).num_minutes());
        assert_eq!(8, linear(Duration::minutes(4), 2).num_minutes());
        assert_eq!(12, linear(Duration::minutes(4), 3).num_minutes());
        assert_eq!(16, linear(Duration::minutes(4), 4).num_minutes());
    }
}
