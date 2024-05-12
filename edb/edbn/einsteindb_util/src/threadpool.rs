// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::collections::VecDeque;
use std::fmt::Write;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{Builder, JoinHandle};
use std::time::Duration;
use std::usize;

pub const DEFAULT_TASKS_PER_TICK: usize = 10000;
const DEFAULT_QUEUE_CAPACITY: usize = 1000;
const DEFAULT_THREAD_COUNT: usize = 1;
const NAP_SECS: u64 = 1;
const QUEUE_MAX_CAPACITY: usize = 8 * DEFAULT_QUEUE_CAPACITY;

pub trait Context: lightlike {
    fn on_task_spacelikeed(&mut self) {}
    fn on_task_finished(&mut self) {}
    fn on_tick(&mut self) {}
}

#[derive(Default)]
pub struct DefaultContext;

impl Context for DefaultContext {}

pub trait ContextFactory<Ctx: Context> {
    fn create(&self) -> Ctx;
}

pub struct DefaultContextFactory;

impl<C: Context + Default> ContextFactory<C> for DefaultContextFactory {
    fn create(&self) -> C {
        C::default()
    }
}

pub struct Task<C> {
    task: Box<dyn FnOnce(&mut C) + lightlike>,
}

impl<C: Context> Task<C> {
    fn new<F>(job: F) -> Task<C>
    where
        for<'r> F: FnOnce(&'r mut C) + lightlike + 'static,
    {
        Task {
            task: Box::new(job),
        }
    }
}

// First in first out queue.
pub struct FifoQueue<C> {
    queue: VecDeque<Task<C>>,
}

impl<C: Context> FifoQueue<C> {
    fn new() -> FifoQueue<C> {
        FifoQueue {
            queue: VecDeque::with_capacity(DEFAULT_QUEUE_CAPACITY),
        }
    }

    fn push(&mut self, task: Task<C>) {
        self.queue.push_back(task);
    }

    fn pop(&mut self) -> Option<Task<C>> {
        let task = self.queue.pop_front();

        if self.queue.is_empty() && self.queue.capacity() > QUEUE_MAX_CAPACITY {
            self.queue = VecDeque::with_capacity(DEFAULT_QUEUE_CAPACITY);
        }

        task
    }
}

pub struct ThreadPoolBuilder<C, F> {
    name: String,
    thread_count: usize,
    tasks_per_tick: usize,
    stack_size: Option<usize>,
    factory: F,
    _ctx: PhantomData<C>,
}

impl<C: Context + Default + 'static> ThreadPoolBuilder<C, DefaultContextFactory> {
    pub fn with_default_factory(name: String) -> ThreadPoolBuilder<C, DefaultContextFactory> {
        ThreadPoolBuilder::new(name, DefaultContextFactory)
    }
}

impl<C: Context + 'static, F: ContextFactory<C>> ThreadPoolBuilder<C, F> {
    pub fn new(name: String, factory: F) -> ThreadPoolBuilder<C, F> {
        ThreadPoolBuilder {
            name,
            thread_count: DEFAULT_THREAD_COUNT,
            tasks_per_tick: DEFAULT_TASKS_PER_TICK,
            stack_size: None,
            factory,
            _ctx: PhantomData,
        }
    }

    pub fn thread_count(mut self, count: usize) -> ThreadPoolBuilder<C, F> {
        self.thread_count = count;
        self
    }

    pub fn tasks_per_tick(mut self, count: usize) -> ThreadPoolBuilder<C, F> {
        self.tasks_per_tick = count;
        self
    }

    pub fn stack_size(mut self, size: usize) -> ThreadPoolBuilder<C, F> {
        self.stack_size = Some(size);
        self
    }

    pub fn build(self) -> ThreadPool<C> {
        ThreadPool::new(
            self.name,
            self.thread_count,
            self.tasks_per_tick,
            self.stack_size,
            self.factory,
        )
    }
}

struct ScheduleState<Ctx> {
    queue: FifoQueue<Ctx>,
    stopped: bool,
}

pub struct Interlock_Semaphore<Ctx> {
    state: Arc<(Mutex<ScheduleState<Ctx>>, Condvar)>,
    task_count: Arc<AtomicUsize>,
}

impl<Ctx> Interlock_Semaphore<Ctx> {
    pub fn schedule<F>(&self, job: F)
    where
        F: FnOnce(&mut Ctx) + lightlike + 'static,
        Ctx: Context,
    {
        let task = Task::new(job);
        let &(ref dagger, ref cvar) = &*self.state;
        {
            let mut state = dagger.dagger().unwrap();
            if state.stopped {
                return;
            }
            state.queue.push(task);
            cvar.notify_one();
        }
        self.task_count.fetch_add(1, AtomicOrdering::SeqCst);
    }
}

impl<Ctx> Clone for Interlock_Semaphore<Ctx> {
    fn clone(&self) -> Self {
        Interlock_Semaphore {
            state: self.state.clone(),
            task_count: self.task_count.clone(),
        }
    }
}

/// `ThreadPool` is used to execute tasks in parallel.
/// Each task would be pushed into the pool, and when a thread
/// is ready to process a task, it will get a task from the pool
/// according to the `ScheduleQueue` provided in initialization.
pub struct ThreadPool<Ctx> {
    threads: Vec<JoinHandle<()>>,
    interlock_semaphore: Interlock_Semaphore<Ctx>,
}

impl<Ctx> ThreadPool<Ctx>
where
    Ctx: Context + 'static,
{
    fn new<C: ContextFactory<Ctx>>(
        name: String,
        num_threads: usize,
        tasks_per_tick: usize,
        stack_size: Option<usize>,
        f: C,
    ) -> ThreadPool<Ctx> {
        assert!(num_threads >= 1);
        let state = ScheduleState {
            queue: FifoQueue::new(),
            stopped: false,
        };
        let state = Arc::new((Mutex::new(state), Condvar::new()));
        let mut threads = Vec::with_capacity(num_threads);
        let task_count = Arc::new(AtomicUsize::new(0));
        // Threadpool threads
        for _ in 0..num_threads {
            let state = Arc::clone(&state);
            let task_num = Arc::clone(&task_count);
            let ctx = f.create();
            let mut tb = Builder::new().name(name.clone());
            if let Some(stack_size) = stack_size {
                tb = tb.stack_size(stack_size);
            }
            let thread = tb
                .spawn(move || {
                    edb_alloc::add_thread_memory_accessor();
                    let mut worker = Worker::new(state, task_num, tasks_per_tick, ctx);
                    worker.run();
                    edb_alloc::remove_thread_memory_accessor();
                })
                .unwrap();
            threads.push(thread);
        }

        ThreadPool {
            threads,
            interlock_semaphore: Interlock_Semaphore { state, task_count },
        }
    }

    pub fn interlock_semaphore(&self) -> Interlock_Semaphore<Ctx> {
        self.interlock_semaphore.clone()
    }

    pub fn execute<F>(&self, job: F)
    where
        F: FnOnce(&mut Ctx) + lightlike + 'static,
        Ctx: Context,
    {
        self.interlock_semaphore.schedule(job)
    }

    #[inline]
    pub fn get_task_count(&self) -> usize {
        self.interlock_semaphore.task_count.load(AtomicOrdering::SeqCst)
    }

    pub fn stop(&mut self) -> Result<(), String> {
        let &(ref dagger, ref cvar) = &*self.interlock_semaphore.state;
        {
            let mut state = dagger.dagger().unwrap();
            state.stopped = true;
            cvar.notify_all();
        }
        let mut err_msg = String::new();
        for t in self.threads.drain(..) {
            if let Err(e) = t.join() {
                write!(&mut err_msg, "Failed to join thread with err: {:?};", e).unwrap();
            }
        }
        if !err_msg.is_empty() {
            return Err(err_msg);
        }
        Ok(())
    }
}

// Each thread has a worker.
struct Worker<C> {
    state: Arc<(Mutex<ScheduleState<C>>, Condvar)>,
    task_count: Arc<AtomicUsize>,
    tasks_per_tick: usize,
    task_counter: usize,
    ctx: C,
}

impl<C> Worker<C>
where
    C: Context,
{
    fn new(
        state: Arc<(Mutex<ScheduleState<C>>, Condvar)>,
        task_count: Arc<AtomicUsize>,
        tasks_per_tick: usize,
        ctx: C,
    ) -> Worker<C> {
        Worker {
            state,
            task_count,
            tasks_per_tick,
            task_counter: 0,
            ctx,
        }
    }

    fn next_task(&mut self) -> Option<Task<C>> {
        let &(ref dagger, ref cvar) = &*self.state;
        let mut state = dagger.dagger().unwrap();
        let mut timeout = Some(Duration::from_secs(NAP_SECS));
        loop {
            if state.stopped {
                return None;
            }
            match state.queue.pop() {
                Some(t) => {
                    self.task_counter += 1;
                    return Some(t);
                }
                None => {
                    state = match timeout {
                        Some(t) => cvar.wait_timeout(state, t).unwrap().0,
                        None => {
                            self.task_counter = 0;
                            self.ctx.on_tick();
                            cvar.wait(state).unwrap()
                        }
                    };
                    timeout = None;
                }
            }
        }
    }

    fn run(&mut self) {
        loop {
            let task = match self.next_task() {
                None => return,
                Some(t) => t,
            };

            self.ctx.on_task_spacelikeed();
            (task.task)(&mut self.ctx);
            self.ctx.on_task_finished();
            self.task_count.fetch_sub(1, AtomicOrdering::SeqCst);
            if self.task_counter == self.tasks_per_tick {
                self.task_counter = 0;
                self.ctx.on_tick();
            }
        }
    }
}

#[causet(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicIsize, Ordering};
    use std::sync::mpsc::{channel, lightlikeer};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn test_get_task_count() {
        let name = thd_name!("test_get_task_count");
        let mut task_pool = ThreadPoolBuilder::with_default_factory(name).build();
        let (tx, rx) = channel();
        let (ftx, frx) = channel();
        let receiver = Arc::new(Mutex::new(rx));
        let timeout = Duration::from_secs(2);
        let group_num = 4;
        let mut task_num = 0;
        for gid in 0..group_num {
            let rxer = Arc::clone(&receiver);
            let ftx = ftx.clone();
            task_pool.execute(move |_: &mut DefaultContext| {
                let rx = rxer.dagger().unwrap();
                let id = rx.recv_timeout(timeout).unwrap();
                assert_eq!(id, gid);
                ftx.lightlike(true).unwrap();
            });
            task_num += 1;
            assert_eq!(task_pool.get_task_count(), task_num);
        }

        for gid in 0..group_num {
            tx.lightlike(gid).unwrap();
            frx.recv_timeout(timeout).unwrap();
            let left_num = task_pool.get_task_count();
            // current task may be still running.
            assert!(
                left_num == task_num || left_num == task_num - 1,
                format!("left_num {},task_num {}", left_num, task_num)
            );
            task_num -= 1;
        }
        task_pool.stop().unwrap();
    }

    #[test]
    fn test_interlock_semaphore() {
        let name = thd_name!("test_interlock_semaphore");
        let mut task_pool = ThreadPoolBuilder::with_default_factory(name).build();
        let interlock_semaphore = task_pool.interlock_semaphore();
        let (tx, rx) = channel();

        for _ in 0..10 {
            let t = tx.clone();
            interlock_semaphore.schedule(move |_: &mut DefaultContext| {
                t.lightlike(()).unwrap();
            });
        }

        for _ in 0..10 {
            rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        rx.recv_timeout(Duration::from_secs(1)).unwrap_err();
        task_pool.stop().unwrap();
    }

    #[test]
    fn test_task_context() {
        struct TestContext {
            counter: Arc<AtomicIsize>,
            tx: lightlikeer<()>,
        }

        unsafe impl lightlike for TestContext {}

        impl Context for TestContext {
            fn on_task_spacelikeed(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
            }
            fn on_task_finished(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
                self.tx.lightlike(()).unwrap();
            }
            fn on_tick(&mut self) {}
        }

        struct TestContextFactory {
            counter: Arc<AtomicIsize>,
            tx: lightlikeer<()>,
        }

        impl ContextFactory<TestContext> for TestContextFactory {
            fn create(&self) -> TestContext {
                TestContext {
                    counter: Arc::clone(&self.counter),
                    tx: self.tx.clone(),
                }
            }
        }

        let (tx, rx) = channel();

        let f = TestContextFactory {
            counter: Arc::new(AtomicIsize::new(0)),
            tx,
        };
        let ctx = f.create();
        let name = thd_name!("test_tasks_with_contexts");
        let mut task_pool = ThreadPoolBuilder::new(name, f).thread_count(5).build();

        for _ in 0..10 {
            task_pool.execute(move |_: &mut TestContext| {});
        }
        for _ in 0..10 {
            rx.recv_timeout(Duration::from_millis(20)).unwrap();
        }
        task_pool.stop().unwrap();
        assert_eq!(ctx.counter.load(Ordering::SeqCst), 20);
    }

    #[test]
    fn test_task_tick() {
        struct TestContext {
            counter: Arc<AtomicIsize>,
            tx: lightlikeer<()>,
        }

        unsafe impl lightlike for TestContext {}

        impl Context for TestContext {
            fn on_task_spacelikeed(&mut self) {}
            fn on_task_finished(&mut self) {}
            fn on_tick(&mut self) {
                self.counter.fetch_add(1, Ordering::SeqCst);
                let _ = self.tx.lightlike(());
            }
        }

        struct TestContextFactory {
            counter: Arc<AtomicIsize>,
            tx: lightlikeer<()>,
        }

        impl ContextFactory<TestContext> for TestContextFactory {
            fn create(&self) -> TestContext {
                TestContext {
                    counter: Arc::clone(&self.counter),
                    tx: self.tx.clone(),
                }
            }
        }

        let (tx, rx) = channel();

        let f = TestContextFactory {
            counter: Arc::new(AtomicIsize::new(0)),
            tx,
        };
        let ctx = f.create();
        let name = thd_name!("test_tasks_tick");
        let mut task_pool = ThreadPoolBuilder::new(name, f)
            .thread_count(5)
            .tasks_per_tick(1)
            .build();

        for _ in 0..10 {
            task_pool.execute(move |_: &mut TestContext| {});
        }
        for _ in 0..10 {
            rx.recv_timeout(Duration::from_millis(100)).unwrap();
        }
        task_pool.stop().unwrap();
        // `on_tick` may be called even if there is no task.
        assert!(ctx.counter.load(Ordering::SeqCst) >= 10);
    }
}
