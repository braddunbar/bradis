#[cfg(feature = "tokio-runtime")]
mod tokio;
#[cfg(feature = "tokio-runtime")]
pub use tokio::*;

#[cfg(not(feature = "tokio-runtime"))]
mod futures;
#[cfg(not(feature = "tokio-runtime"))]
pub use futures::*;
