pub mod naive_pool;

pub trait ThreadPool {
    fn new(pool_size: u32) -> Self;

    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + 'static;
}