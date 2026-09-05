// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "group0/group0_sync.h"

#include "crowdb-kv-client/c_api.h"
#include "disk/block_disk.h"
#include "disk/mem_disk.h"
#include "disk/null_disk.h"
#include "engine/uring/uring_engine.h"

#include <folly/dynamic.h>
#include <folly/json.h>

#include <cstdio>
#include <cstring>
#include <thread>
#include <unordered_set>

namespace crowdb::diskio
{

// ── Callback context for async FFI ops ────────────────────────────

struct SyncCallbackCtx
{
    std::atomic<int>  status{-1};
    std::string       json_result;
    std::atomic<bool> done{false};
};

static void on_ffi_complete(int status, const char *result_json, void *user_data)
{
    auto *ctx = static_cast<SyncCallbackCtx *>(user_data);
    ctx->status.store(status, std::memory_order_relaxed);
    if (result_json != nullptr) {
        ctx->json_result = result_json;
    }
    ctx->done.store(true, std::memory_order_release);
}

static bool wait_for_ctx(SyncCallbackCtx &ctx, uint32_t timeout_ms = 10000)
{
    for (uint32_t i = 0; i < timeout_ms / 10; ++i) {
        if (ctx.done.load(std::memory_order_acquire)) {
            return ctx.status.load(std::memory_order_relaxed) == 0;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

// ── Group0Sync ────────────────────────────────────────────────────

Group0Sync::Group0Sync(Group0SyncConfig cfg, std::shared_ptr<DiskSet> disk_set, std::shared_ptr<IoEngine> engine,
                       crowdb::rpc::ScheduledExecutor &executor)
    : cfg_(std::move(cfg)),
      disk_set_(std::move(disk_set)),
      engine_(std::move(engine)),
      executor_(executor)
{
}

Group0Sync::~Group0Sync()
{
    stop();
    if (hw_client_ != nullptr) {
        crowdb_hw_client_destroy(hw_client_);
    }
    if (svc_client_ != nullptr) {
        crowdb_svc_client_destroy(svc_client_);
    }
}

void Group0Sync::start()
{
    // Create FFI clients.
    std::vector<const char *> seeds;
    for (const auto &s : cfg_.kv_seeds) {
        seeds.push_back(s.c_str());
    }
    hw_client_  = crowdb_hw_client_create(seeds.data(), seeds.size());
    svc_client_ = crowdb_svc_client_create(seeds.data(), seeds.size());

    if (hw_client_ == nullptr || svc_client_ == nullptr) {
        std::fprintf(stderr, "warning: failed to create group-0 clients, sync disabled\n");
        return;
    }

    // Schedule the first sync immediately (delay=0).
    schedule_next_sync();
}

void Group0Sync::stop()
{
    if (sync_task_id_ != 0) {
        executor_.cancel(sync_task_id_);
        sync_task_id_ = 0;
    }
}

void Group0Sync::schedule_next_sync()
{
    sync_task_id_ = executor_.schedule(
        [this] {
            do_sync();
            schedule_next_sync();
        },
        cfg_.sync_interval_ms);
}

void Group0Sync::do_sync()
{
    if (cfg_.auto_discover_disks) {
        fetch_disks_from_group0();
    }
    heartbeat();
}

void Group0Sync::fetch_disks_from_group0()
{
    if (hw_client_ == nullptr) {
        return;
    }

    SyncCallbackCtx ctx;
    crowdb_hw_list_disks_in_group(hw_client_, cfg_.rack_id, cfg_.node_id, cfg_.dg_id, on_ffi_complete, &ctx);
    if (!wait_for_ctx(ctx)) {
        std::fprintf(stderr, "warning: group-0 list_disks timed out\n");
        return;
    }
    if (ctx.status.load() != 0) {
        std::fprintf(stderr, "warning: group-0 list_disks failed\n");
        return;
    }

    reconcile_disks(ctx.json_result);
}

void Group0Sync::reconcile_disks(const std::string &json)
{
    // Parse the JSON array of {"disk_id": {"high": N, "low": N}, "value": {...}}.
    folly::dynamic parsed;
    try {
        parsed = folly::parseJson(json);
    }
    catch (const std::exception &e) {
        std::fprintf(stderr, "warning: group-0 disk list JSON parse error: %s\n", e.what());
        return;
    }

    if (!parsed.isArray()) {
        std::fprintf(stderr, "warning: group-0 disk list is not a JSON array\n");
        return;
    }

    // Collect the disk IDs from group-0 and create/update disks.
    std::unordered_set<DiskId, DiskIdHash> seen_ids;
    for (const auto &entry : parsed) {
        if (!entry.isObject()) {
            continue;
        }
        const auto &did_obj = entry["disk_id"];
        const auto &val_obj = entry["value"];
        if (!did_obj.isObject() || !val_obj.isObject()) {
            continue;
        }

        DiskId did;
        did.high = did_obj["high"].asInt();
        did.low  = did_obj["low"].asInt();
        seen_ids.insert(did);

        // Check if this disk already exists in the DiskSet.
        auto existing = disk_set_->find_disk(did);
        if (existing != nullptr) {
            // Disk already present — no update needed (disks are immutable
            // once opened; zone layout doesn't change at runtime).
            continue;
        }

        // Create a new disk from the DiskValue fields.
        std::string device_path     = val_obj["device_path"].asString();
        uint64_t    zone_size_units = val_obj["zone_size_units"].asInt();
        uint32_t    unit_size       = static_cast<uint32_t>(val_obj["unit_size_bytes"].asInt());
        uint32_t    zone_count      = static_cast<uint32_t>(val_obj["zone_count"].asInt());

        // Build zones: one zone covering the full capacity (simple layout).
        std::vector<Zone> zones;
        for (uint32_t i = 0; i < zone_count; ++i) {
            Zone z;
            z.zone_index  = i;
            z.base_offset = static_cast<int64_t>(i) * static_cast<int64_t>(zone_size_units * unit_size);
            z.capacity    = static_cast<int64_t>(zone_size_units * unit_size);
            zones.push_back(z);
        }

        std::shared_ptr<Disk> disk;
        if (device_path.empty()) {
            // No device path — dummy disk. Use the configured type
            // (NullDisk for benchmarks, MemDisk for correctness tests).
            if (cfg_.dummy_disk_type == DummyDiskType::Mem) {
                disk = std::make_shared<MemDisk>(did, engine_, std::move(zones), cfg_.dummy_props);
            }
            else {
                disk = std::make_shared<NullDisk>(did, engine_, std::move(zones), cfg_.dummy_props);
            }
        }
        else {
            // Real block device.
            disk = std::make_shared<BlockDisk>(did, device_path, engine_, std::move(zones), true);
        }
#ifdef CROWDB_HAVE_LIBURING
        // Register the disk's fd with the uring for fd→pipeline routing.
        // All disk types go through the uring submit path; unregistered
        // fds fall back to pipeline 0 but their completions are not
        // reliably delivered.
        if (auto uring_engine = std::dynamic_pointer_cast<UringEngine>(engine_);
            uring_engine != nullptr && disk->fd() >= 0) {
            uring_engine->uring().register_fd(disk->fd());
        }
#endif
        disk_set_->add(disk);
        std::printf("group-0: added disk {%llu,%llu} path=%s zones=%u\n", static_cast<unsigned long long>(did.high),
                    static_cast<unsigned long long>(did.low), device_path.empty() ? "(null)" : device_path.c_str(),
                    zone_count);
    }

    // Remove disks that are no longer in group-0.
    // In-flight IO on removed disks completes safely (shared_ptr keeps
    // the Disk alive); new requests get DiskNotExist.
    for (const auto &existing_id : disk_set_->disk_ids()) {
        if (seen_ids.find(existing_id) == seen_ids.end()) {
            disk_set_->remove_disk(existing_id);
            std::printf("group-0: removed disk {%llu,%llu} (no longer in group-0)\n",
                        static_cast<unsigned long long>(existing_id.high),
                        static_cast<unsigned long long>(existing_id.low));
        }
    }
}

void Group0Sync::heartbeat()
{
    if (svc_client_ == nullptr) {
        return;
    }

    std::string dg_ids_json = "[" + std::to_string(cfg_.dg_id) + "]";

    SyncCallbackCtx ctx;
    crowdb_svc_heartbeat_diskio(svc_client_, cfg_.instance_id, cfg_.rpc_endpoint.c_str(), dg_ids_json.c_str(), "[]",
                                on_ffi_complete, &ctx);
    if (!wait_for_ctx(ctx)) {
        std::fprintf(stderr, "warning: group-0 heartbeat timed out\n");
        return;
    }
    if (ctx.status.load() != 0) {
        std::fprintf(stderr, "warning: group-0 heartbeat failed\n");
    }
}

} // namespace crowdb::diskio
