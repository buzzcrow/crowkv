// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Safe wrappers for Buffer and BufferPool.

use crate::sys;
use std::ptr;

/// A pool-allocated byte buffer. The buffer is ref-counted; `Drop` calls
/// `release` which decrements the refcount and recycles to the pool when
/// it hits zero.
pub struct Buffer {
    handle: sys::crowdb_rpc_buffer_t,
}

impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl Buffer {
    /// Allocate a new buffer from the pool with the given capacity.
    /// Returns `None` if the pool is exhausted.
    pub fn alloc(pool: &BufferPool, capacity: u32) -> Option<Self> {
        let handle = unsafe { sys::crowdb_rpc_buffer_alloc(pool.handle, capacity) };
        if handle.is_null() {
            None
        } else {
            Some(Buffer { handle })
        }
    }

    /// Write data into the buffer. Called once per buffer (write-once).
    pub fn write(&mut self, data: &[u8]) {
        unsafe {
            sys::crowdb_rpc_buffer_write(self.handle, data.as_ptr(), data.len() as u32);
        }
    }

    /// Read-only access to the buffer's data.
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            let ptr = sys::crowdb_rpc_buffer_data(self.handle);
            let len = sys::crowdb_rpc_buffer_len(self.handle);
            if ptr.is_null() || len == 0 {
                &[]
            } else {
                std::slice::from_raw_parts(ptr, len as usize)
            }
        }
    }

    /// Take ownership of the handle (prevents Drop from releasing it).
    pub fn into_raw(mut self) -> sys::crowdb_rpc_buffer_t {
        let h = self.handle;
        self.handle = ptr::null_mut();
        h
    }

    /// Create a Buffer from a raw handle (takes ownership).
    pub fn from_raw(handle: sys::crowdb_rpc_buffer_t) -> Self {
        Buffer { handle }
    }

    /// Create a standalone buffer (not pool-allocated) from raw bytes.
    /// The buffer owns a malloc'd copy; Drop releases it.
    pub fn from_bytes(data: &[u8]) -> Self {
        let handle = unsafe { sys::crowdb_rpc_buffer_create(data.as_ptr(), data.len() as u32) };
        Buffer { handle }
    }

    /// Create a standalone buffer from a Vec (copies into malloc'd memory).
    pub fn from_vec(data: Vec<u8>) -> Self {
        Self::from_vec_offset(data, 0)
    }

    /// Wrap immutable owned bytes without copying their allocation.
    pub fn from_owned_bytes(data: bytes::Bytes) -> Self {
        if data.is_empty() {
            return Buffer {
                handle: std::ptr::null_mut(),
            };
        }
        let data_ptr = data.as_ptr();
        let len = data.len();
        let ctx = Box::into_raw(Box::new(data)).cast::<std::ffi::c_void>();
        let handle =
            unsafe { sys::crowdb_rpc_buffer_create_external(data_ptr, len as u32, Some(free_bytes_cb), ctx) };
        Buffer { handle }
    }

    /// Create an external buffer wrapping a Vec allocation (zero-copy).
    /// The buffer's `data` points to `vec[head..]`; the entire Vec
    /// allocation is kept alive until the Buffer is released, at which
    /// point the Vec is dropped. No copy occurs.
    ///
    /// Use `head = 0` to wrap the entire Vec, or `head > 0` when using
    /// `FlatBufferBuilder::collapse()` (which returns the finished data
    /// at offset `head` within the internal Vec).
    pub fn from_vec_offset(vec: Vec<u8>, head: usize) -> Self {
        let len = vec.len().saturating_sub(head);
        if len == 0 {
            // Empty — no buffer needed, just drop the Vec.
            drop(vec);
            // Create a null handle — submit_response_buffer handles null.
            return Buffer {
                handle: std::ptr::null_mut(),
            };
        }
        let data_ptr = vec.as_ptr().wrapping_add(head);
        // Box the Vec to get a stable pointer for the free callback.
        // The callback drops the Box<Vec<u8>>, freeing the allocation.
        let boxed = Box::new(vec);
        let ctx = Box::into_raw(boxed).cast::<std::ffi::c_void>();
        let handle =
            unsafe { sys::crowdb_rpc_buffer_create_external(data_ptr, len as u32, Some(free_vec_cb), ctx) };
        Buffer { handle }
    }

    /// Read-only access to the buffer's data as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        self.bytes()
    }

    /// Returns true if this Buffer has no underlying C++ handle (e.g.
    /// created from an empty Vec via `from_vec_offset`).
    pub fn is_null_handle(&self) -> bool {
        self.handle.is_null()
    }
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::crowdb_rpc_buffer_release(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}

// Buffer is Send (C++ buffers are thread-safe via atomic refcount).
// Not Sync (the write path is single-threaded per buffer).
unsafe impl Send for Buffer {}

/// A buffer pool. Allocates and recycles Buffer objects.
pub struct BufferPool {
    handle: sys::crowdb_rpc_pool_t,
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("handle", &self.handle)
            .finish_non_exhaustive()
    }
}

impl BufferPool {
    /// Create a new pool with the given max buffer count.
    pub fn new(max_buffers: u32) -> Self {
        let handle = unsafe { sys::crowdb_rpc_pool_create(max_buffers) };
        BufferPool { handle }
    }

    /// Allocate a buffer from this pool.
    pub fn alloc_buffer(&self, capacity: u32) -> Option<Buffer> {
        Buffer::alloc(self, capacity)
    }

    pub(crate) fn handle(&self) -> sys::crowdb_rpc_pool_t {
        self.handle
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { sys::crowdb_rpc_pool_destroy(self.handle) };
            self.handle = ptr::null_mut();
        }
    }
}

// Safety: BufferPool wraps a C++ handle that is safe to share across
// threads (the pool uses a mutex-protected free list + atomic refcounts).
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

// Free callback for external buffers created via `Buffer::from_vec_offset`.
// Drops the Box<Vec<u8>>, freeing the allocation that the Buffer's `data`
// pointer points into.
extern "C" fn free_vec_cb(ctx: *mut std::ffi::c_void) {
    if !ctx.is_null() {
        // SAFETY: ctx was created by Box::into_raw(Box::new(Vec<u8>)) in
        // from_vec_offset. We reclaim it here exactly once (the C++ Buffer
        // release() calls free_cb once when refcount hits zero).
        unsafe { drop(Box::from_raw(ctx.cast::<Vec<u8>>())) };
    }
}

extern "C" fn free_bytes_cb(ctx: *mut std::ffi::c_void) {
    if !ctx.is_null() {
        // SAFETY: `ctx` came from `Box::into_raw(Box::new(Bytes))` above and
        // the C++ buffer invokes its release callback exactly once.
        unsafe { drop(Box::from_raw(ctx.cast::<bytes::Bytes>())) };
    }
}
