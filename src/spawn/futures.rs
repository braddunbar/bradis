use futures::{
    executor::{LocalPool, LocalSpawner},
    future::RemoteHandle,
    task::SpawnExt,
};
use std::{cell::RefCell, future::Future};

thread_local! {
    static POOL: RefCell<LocalPool> = RefCell::new(LocalPool::new());
    static SPAWNER: RefCell<LocalSpawner> = RefCell::new(POOL.with(|pool| pool.borrow().spawner()));
}

pub fn spawn_with_handle<F>(f: F) -> TaskHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let handle = SPAWNER.with(|s| s.borrow_mut().spawn_with_handle(f));
    TaskHandle(Some(handle.unwrap()))
}

pub fn spawn<F>(f: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    SPAWNER.with(|s| s.borrow_mut().spawn(f).unwrap())
}

pub struct TaskHandle<T>(Option<RemoteHandle<T>>);

impl<T: Send + 'static> TaskHandle<T> {
    pub fn abort(&mut self) {
        self.0.take();
    }
}

pub fn run_until_stalled() {
    POOL.with(|pool| pool.borrow_mut().run_until_stalled());
}
