use std::sync::atomic::{Ordering, AtomicPtr};
use std::ptr;

struct Node<T> {
    value: T,
    next: *mut Self,
}

pub struct Stack<T> {
    top: AtomicPtr<Node<T>>,
}

impl<T> Stack<T> {
    pub fn new() -> Self {
        Self { top: AtomicPtr::new(ptr::null_mut()) }
    }

    pub fn push(&self, value: T) {
        let next = self.top.load(Ordering::Relaxed);
        let node = Box::into_raw(Box::new(Node { value, next }));

        // SAFETY: There is no other thread acccessing the node we are trying
        // to push. So it is safe to access and modify it via pointer.
        // However, once we append it, it is no longer safe to do so.
        unsafe {
            while let Err(top) = self.top.compare_exchange_weak((*node).next, node, Ordering::Relaxed, Ordering::Relaxed) {
                (*node).next = top;
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        let mut top = self.top.load(Ordering::Relaxed);

        loop {
            if top.is_null() { return None }

            // SAFETY: None of the popping threads will ever mutate a
            // *non-exclusive* node through a pointer. So reading is safe.
            let next = unsafe { (*top).next };

            match self.top.compare_exchange_weak(top, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(node) => top = node,
            }
        }

        // SAFETY: The CAS loop has succeded, this means that this thread is the
        // only one that popped the top node, and responsible for returning the
        // value of the top node to the caller. However, there may stil be more
        // than one reader to the top node through pointers, so it not safe to
        // deallocate the top node, and that is a reclamation problem, we are
        // reading the pointer and return the owned value to the caller.
        let node = unsafe { ptr::read(top) };

        Some(node.value)
    }
}

#[cfg(test)]
mod test {
    use std::num::Wrapping;
    use super::Stack;
    use std::thread;

    #[test]
    fn push_then_pop() {
        const NUM_THREADS: usize = 1_000;
        const NUM_PUSH_PER_THREAD: usize = 10_000;

        let stack = Stack::<usize>::new();

        thread::scope(|scope| {
            let mut handles = Vec::new();
            
            // Spawn NUM_THREADS, each of them locally adding NUM_PUSH_PER_THREAD
            // random numbers, and pushing them to the stack.
            for _ in 0..NUM_THREADS {
                handles.push(
                    scope.spawn(|| {
                        let mut sum = Wrapping(0);

                        for _ in 0..NUM_PUSH_PER_THREAD {
                            let random = rand::random::<usize>();
                            sum += random;
                            stack.push(random);
                        }

                        sum
                    })
                );
            }

            // Add all local sums.
            let thread_sum = handles
                .drain(..)
                .map(|handle| handle.join().expect("no panics"))
                .fold(Wrapping(0), |a, b| a + b);

            // Create NUM_THREADS, each of them popping random numbers and
            // adding them locally.
            for _ in 0..NUM_THREADS {
                handles.push(
                    scope.spawn(|| {
                        let mut product = Wrapping(0);

                        while let Some(number) = stack.pop() {
                            product += number;
                        }

                        product
                    })
                );
            }

            // Add all local pop sums.
            let pop_sum = handles
                .into_iter()
                .map(|handle| handle.join().expect("no panics"))
                .fold(Wrapping(0), |a, b| a + b);

            // Check if they are equal, this is not allowed to panic.
            assert_eq!(pop_sum, thread_sum);
        })
    }
}
