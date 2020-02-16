use std::panic::UnwindSafe;

pub mod naive_pool;
pub mod queue_pool;

pub trait ThreadPool {
    fn new(pool_size: u32) -> Self;

    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + UnwindSafe + 'static;
}