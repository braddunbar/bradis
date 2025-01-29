use std::time::{Duration, UNIX_EPOCH};

pub fn epoch() -> Duration {
    UNIX_EPOCH
        .elapsed()
        .expect("current time is before unix epoch")
}
