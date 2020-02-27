use std::thread;
use std::sync::{Arc, mpsc, Mutex};
use std::thread::JoinHandle;

use log::{debug, error};

use crate::thread_pool::ThreadPool;
use std::panic::{catch_unwind, UnwindSafe};

type Job = Box<dyn FnOnce() + Send>;


struct Worker {
    id : u32,
    handler: JoinHandle<()>,
}

impl Worker {
    fn new(id: u32, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Self {
        let handler = thread::spawn(move || {
            loop {
                let job = receiver
                    .lock()
                    .unwrap()
                    .recv()
                    .unwrap();
                match job {
                    Message::New(job) => {
                        debug!("New job for worker #{}", id);
                        //todo replace catch_unwind to respawning thread
                        //if let Err(e) = catch_unwind(job) {
                        //    error!("Panic recovery at worker #{}: {:?}", id, e);
                        //}
                    },
                    Message::Shutdown => {
                        debug!("Shutdown worker #{}", id);
                        break;
                    },
                }
            }
        });
        Worker {id, handler}
    }
}

enum Message {
    New(Job),
    Shutdown,
}

pub struct QueueThreadPool {
    workers : Vec<Option<Worker>>,
    sender: mpsc::Sender<Message>,
}

impl ThreadPool for QueueThreadPool {
    fn new(threads_num: u32) -> Self {
        let (sender, receiver) = mpsc::channel::<Message>();
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(threads_num as usize);
        for i in 0..threads_num {
            workers.push(Some(Worker::new(i, Arc::clone(&receiver))));
        }

        QueueThreadPool { workers, sender }
    }

    fn spawn<F>(&self, f: F)
        where
            F: FnOnce() + Send + 'static
    {
        self.sender.send(Message::New(Box::new(f))).unwrap();
    }
}

impl Drop for QueueThreadPool {
    fn drop(&mut self) {
        debug!("Shutdown thread pool and {} workers", self.workers.len());
        for _ in &self.workers {
            self.sender.send(Message::Shutdown).unwrap();
        }

        for worker in &mut self.workers {
            if let Some(worker) = worker.take() {
                debug!("Shutdown worker #{}", worker.id);
                worker.handler.join().unwrap();
            }
        }
    }
}