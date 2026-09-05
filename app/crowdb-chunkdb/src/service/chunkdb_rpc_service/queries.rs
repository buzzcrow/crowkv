use super::{
    build_list_response, map_error, submit_chunk_result, submit_error, submit_fb_response, Arc, ChunkId,
    ChunkdbRpcService, FBChunkdbRetCode, FBListChunksRequest, FBMsgType, FBQueryChunkRequest, RequestGuard,
    RpcServer, ServerRequest,
};

impl ChunkdbRpcService {
    // ── QueryChunk ────────────────────────────────────────────────

    pub(super) fn handle_query(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EQueryChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBQueryChunkRequest>(req.control()) else {
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

            let result = handler.query_chunk(&chunk_id).await;
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

    // ── ListChunks ────────────────────────────────────────────────

    pub(super) fn handle_list(&self, req: ServerRequest, server: &Arc<RpcServer>, mut request: RequestGuard) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EListChunksResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBListChunksRequest>(req.control()) else {
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

            let start_after = fb_req.start_token().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            });
            let max_keys = fb_req.max_keys();

            let result = handler.list_chunks(start_after.as_ref(), max_keys).await;
            match result {
                Ok(chunks) => {
                    request.mark_success();
                    let next_token = chunks.last().and_then(|c| c.id);
                    let has_next = next_token.is_some();
                    let ctrl = build_list_response(
                        req_id,
                        create_nano,
                        FBChunkdbRetCode::Success,
                        None,
                        0,
                        0,
                        &chunks,
                        next_token.as_ref(),
                        has_next,
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
                    let ctrl =
                        build_list_response(req_id, create_nano, code, Some(&msg), rs, re, &[], None, false);
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
}
