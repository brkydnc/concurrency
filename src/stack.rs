use std::sync::atomic::{Ordering, AtomicPtr, AtomicUsize};
use std::ptr;

struct Node<T> {
    value: T,
    next: *mut Self,
}

pub struct Stack<T> {
    /// The pointer pointer to the top of the stack.
    top: AtomicPtr<Node<T>>,

    /// The number of threads currently popping some data.
    pops: AtomicUsize,

    /// The list of nodes to be deleted.
    garbage: AtomicPtr<Node<T>>,
}

impl<T> Stack<T> {
    pub fn new() -> Self {
        Self {
            top: AtomicPtr::new(ptr::null_mut()),
            pops: AtomicUsize::new(0),
            garbage: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Push data into stack, this allocates a new node for the given data.
    pub fn push(&self, value: T) {
        let next = self.top.load(Ordering::Relaxed);
        let node = Box::into_raw(Box::new(Node { value, next }));

        // SAFETY: There is no other thread acccessing the node we are trying
        // to push. So it is safe to access and modify it via pointer.
        // However, once we push the node, it is no longer safe to do so.
        unsafe {
            while let Err(top) = self.top.compare_exchange_weak((*node).next, node, Ordering::Relaxed, Ordering::Relaxed) {
                (*node).next = top;
            }
        }
    }

    /// Pop data from the stack.
    pub fn pop(&self) -> Option<T> {
        self.pops.fetch_add(1, Ordering::SeqCst);

        // Load the top node so that we can CAS.
        let mut top = self.top.load(Ordering::Relaxed);

        loop {
            // Return None if the top pointer is null.
            if top.is_null() { return None }

            // Read the next node after the current one.
            //
            // SAFETY: None of the popping threads will ever mutate a
            // *non-exclusive* node through a pointer, so reading is safe.
            let next = unsafe { (*top).next };

            // If the top pointer is still pointing to the value we've read at
            // the beginning, swap it with the next node, if not try again with
            // the new node.
            match self.top.compare_exchange_weak(top, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(node) => top = node,
            }
        }

        // Read the value of the node we've just popped.
        //
        // SAFETY: The CAS loop has succeeded, meaning, the current thread is
        // the only one that popped the top node, and responsible for returning
        // the value of the top node to the caller.
        let node = unsafe { ptr::read(top) };

        // Reclaim the nodes if we can.
        unsafe { self.reclaim(top) };

        Some(node.value)
    }

    unsafe fn reclaim(&self, node: *mut Node<T>) {
        let pops = self.pops.load(Ordering::SeqCst);

        if pops == 1 {
            // Capture the garbage list.
            let garbage = self.garbage.swap(ptr::null_mut(), Ordering::SeqCst);

            if self.pops.fetch_sub(1, Ordering::SeqCst) != 1 {
                let mut node = garbage;

                while !node.is_null() {
                    let next = (*node).next;
                    let _ = Box::from_raw(node);
                    node = next;
                }
            } else if !garbage.is_null() {
                self.tie(garbage);
            }

            let _ = Box::from_raw(node);
        } else {
            self.tie(node);
            self.pops.fetch_sub(1, Ordering::SeqCst);
        }
    }

    unsafe fn tie(&self, list: *mut Node<T>) {
        let mut last = list;

        // Find the last item in the list.
        while !(*last).next.is_null() {
            last = (*last).next;
        }

        let mut head = self.garbage.load(Ordering::SeqCst);

        loop {
            (*last).next = head;

            match self.garbage.compare_exchange_weak(head, list, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(node) => head = node,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::num::Wrapping;
    use super::Stack;
    use std::thread;

    #[test]
    fn push_then_pop() {
        const NUM_THREADS: usize = 10;
        const NUM_PUSH_PER_THREAD: usize = 10;

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
