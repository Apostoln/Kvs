use std::thread;

use crate::thread_pool::ThreadPool;
use std::panic::UnwindSafe;

pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_pool_size: u32) -> Self {
        NaiveThreadPool {}
    }

    fn spawn<F>(&self, job: F)
        where
            F: FnOnce() + Send + UnwindSafe + 'static
    {
        thread::spawn(job);
    }
}