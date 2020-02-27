use std::panic::UnwindSafe;

mod naive_pool;
mod queue_pool;
mod rayon_pool;

pub use naive_pool::NaiveThreadPool;
pub use queue_pool::QueueThreadPool;
pub use rayon_pool::RayonThreadPool;

pub trait ThreadPool {
    fn new(pool_size: u32) -> Self;

    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + 'static;
}