use crate::thread_pool::ThreadPool;
use std::panic::UnwindSafe;

use rayon;

pub struct RayonThreadPool {
    inner: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(pool_size: u32) -> Self {
        let inner = rayon::ThreadPoolBuilder::new()
            .num_threads(pool_size as usize)
            .build()
            .unwrap();
        RayonThreadPool { inner }
    }

    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + UnwindSafe + 'static
    {
        self.inner.install(job);
    }
}