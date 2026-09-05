use super::{
    build_delete_range_response, map_error, parse_fb_chunk_strip, proto_chunk_type, proto_strip_type,
    submit_chunk_result, submit_error, submit_fb_response, Arc, ChunkId, ChunkdbRpcService,
    FBAllocateChunkRequest, FBAppendChunkRequest, FBChunkdbRetCode, FBDeleteChunkRangeRequest,
    FBDeleteChunkRequest, FBMsgType, FBSealChunkRequest, FBUpdateChunkStripRequest, RequestGuard, RpcServer,
    ServerRequest,
};

impl ChunkdbRpcService {
    // ── AllocateChunk ─────────────────────────────────────────────

    pub(super) fn handle_allocate(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAllocateChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            // Parse the flatbuffer inside the async task — zero-copy from
            // the owned Frame (released when `req` drops at block end).
            let Ok(fb_req) = flatbuffers::root::<FBAllocateChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let chunk_id = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            });
            let Some(strip_type) = proto_strip_type(fb_req.strip_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip_type",
                );
                return;
            };
            let Some(chunk_type) = proto_chunk_type(fb_req.chunk_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid chunk_type",
                );
                return;
            };
            let write_granularity = fb_req.write_granularity();
            let strip_count = fb_req.strip_count();
            let data_num = fb_req.data_num();
            let code_num = fb_req.code_num();
            let copy_count = fb_req.copy_count();

            let result = handler
                .allocate_chunk(
                    chunk_id,
                    write_granularity,
                    strip_count,
                    strip_type,
                    data_num,
                    code_num,
                    copy_count,
                    chunk_type,
                )
                .await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── AppendChunk ───────────────────────────────────────────────

    pub(super) fn handle_append(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAppendChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBAppendChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let Some(strip_type) = proto_strip_type(fb_req.strip_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip_type",
                );
                return;
            };
            let strip_count = fb_req.strip_count();
            let data_num = fb_req.data_num();
            let code_num = fb_req.code_num();
            let copy_count = fb_req.copy_count();
            let strip_size = fb_req.strip_size();

            let result = handler
                .append_chunk(
                    &chunk_id,
                    strip_count,
                    strip_type,
                    data_num,
                    code_num,
                    copy_count,
                    strip_size,
                )
                .await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── SealChunk ─────────────────────────────────────────────────

    pub(super) fn handle_seal(&self, req: ServerRequest, server: &Arc<RpcServer>, mut request: RequestGuard) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ESealChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBSealChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let seal_length = fb_req.seal_length();

            let result = handler.seal_chunk(&chunk_id, seal_length).await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── DeleteChunk ───────────────────────────────────────────────

    pub(super) fn handle_delete(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EDeleteChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBDeleteChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };

            let result = handler.delete_chunk(&chunk_id).await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── DeleteChunkRange ──────────────────────────────────────────

    pub(super) fn handle_delete_range(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EDeleteChunkRangeResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBDeleteChunkRangeRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let offset = fb_req.chunk_offset();
            let size = fb_req.chunk_size();

            let result = handler.delete_chunk_range(&chunk_id, offset, size).await;
            match result {
                Ok(()) => {
                    request.mark_success();
                    let ctrl = build_delete_range_response(
                        req_id,
                        create_nano,
                        FBChunkdbRetCode::Success,
                        None,
                        0,
                        0,
                    );
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                }
                Err(e) => {
                    let (code, msg, rs, re) = map_error(&e);
                    let ctrl = build_delete_range_response(req_id, create_nano, code, Some(&msg), rs, re);
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                }
            }
        });
    }

    // ── UpdateChunkStrip ──────────────────────────────────────────

    pub(super) fn handle_update_strip(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EUpdateChunkStripResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBUpdateChunkStripRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let strip_index = fb_req.strip_index();
            let Some(fb_strip) = fb_req.strip() else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing strip",
                );
                return;
            };
            let Some(strip) = parse_fb_chunk_strip(&fb_strip) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip body",
                );
                return;
            };

            let result = handler.update_chunk_strip(&chunk_id, strip_index, strip).await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }
}
