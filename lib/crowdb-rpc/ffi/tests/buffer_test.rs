// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_rpc_ffi::Buffer;

#[test]
fn owned_bytes_buffer_keeps_the_original_allocation() {
    let bytes = Bytes::from(vec![0x5a; 4096]);
    let original = bytes.as_ptr();
    let buffer = Buffer::from_owned_bytes(bytes);

    assert_eq!(buffer.bytes().as_ptr(), original);
    assert_eq!(buffer.bytes(), &[0x5a; 4096]);
}

#[test]
fn vec_buffer_keeps_the_original_allocation() {
    let data = vec![0xa5; 4096];
    let original = data.as_ptr();
    let buffer = Buffer::from_vec(data);

    assert_eq!(buffer.bytes().as_ptr(), original);
    assert_eq!(buffer.bytes(), &[0xa5; 4096]);
}
