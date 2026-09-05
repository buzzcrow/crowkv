// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// crowdb-diskio: the disk I/O server binary.
//
// Parses config from CLI args, auto-detects the I/O engine (io_uring
// on Linux with liburing, blocking thread-pool otherwise), creates
// DiskSet + IoEngine, registers diskio RPC handlers, and serves until
// SIGTERM/SIGINT.
//
// Usage: crowdb-diskio --port <port> [--bind <addr>]
//   [--dummy-disk null|mem] [--disk <hex_id>:<path>[:<capacity>]]...
//
// Engine is auto-detected — no --engine flag. Disks with an empty path
// are dummy disks (NullDisk by default, MemDisk with --dummy-disk mem).

#include "crowdb-kv-client/c_api.h"
#include "crowdb-rpc/scheduled_executor.h"
#include "crowdb-rpc/server/server.h"
#include "crowdb-rpc/transport/socket_transport.h"
#include "crowdb-common/metrics/system_metrics.h"
#include "dio_config.h"
#include "disk/block_disk.h"
#include "disk/disk_set.h"
#include "disk/mem_disk.h"
#include "disk/null_disk.h"
#include "engine/blocking/blocking_engine.h"
#include "group0/group0_sync.h"
#include "rpc/dio_server.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <ctime>
#include <filesystem>
#include <thread>

#ifdef CROWDB_HAVE_LIBURING
#    include "engine/uring/uring_engine.h"
#endif

static std::atomic<bool> g_running{true};

static void on_signal(int)
{
    g_running.store(false);
}

// ── Metrics logging ─────────────────────────────────────────────

// Format a timestamp as ISO 8601 (matching the Rust metrics runner).
static std::string iso8601_now()
{
    auto now = std::chrono::system_clock::now();
    auto tt  = std::chrono::system_clock::to_time_t(now);
    struct tm tm_buf;
    gmtime_r(&tt, &tm_buf);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm_buf);
    return buf;
}

// Open a metrics log file under <log_dir>/crowdb-diskio-metrics-<pid>.log.
// Returns nullptr on failure (metrics logging is silently disabled).
static FILE *open_metrics_log(const std::string &log_dir)
{
    std::error_code ec;
    std::filesystem::create_directories(log_dir, ec);
    std::string path = log_dir + "/crowdb-diskio-metrics-" + std::to_string(::getpid()) + ".log";
    FILE *fp = std::fopen(path.c_str(), "a");
    if (fp != nullptr) {
        std::setvbuf(fp, nullptr, _IOLBF, 0); // line-buffered
        std::printf("metrics log: %s\n", path.c_str());
    }
    return fp;
}

// Periodic system metrics flush. Reschedules itself on the executor
// every interval_secs to emulate a periodic timer.
static void schedule_metrics_flush(
    crowdb::rpc::ScheduledExecutor &scheduler,
    crowdb::common::metrics::SystemCollector &collector,
    FILE *metrics_fp,
    uint32_t interval_secs)
{
    auto *sched_ptr  = &scheduler;
    auto *coll_ptr   = &collector;
    auto *fp_ptr     = metrics_fp;
    auto  interval   = interval_secs;

    scheduler.schedule(
        [sched_ptr, coll_ptr, fp_ptr, interval]() {
            if (fp_ptr == nullptr)
                return;

            auto snap = coll_ptr->collect();
            auto ts   = iso8601_now();
            std::fprintf(fp_ptr, "[%s window=%.3fs]\n", ts.c_str(),
                         static_cast<double>(interval));
            std::fprintf(fp_ptr, "misc\n");
            crowdb::common::metrics::flush_system(fp_ptr, snap);
            std::fflush(fp_ptr);

            // Reschedule for the next tick.
            schedule_metrics_flush(*sched_ptr, *coll_ptr, fp_ptr, interval);
        },
        interval_secs * 1000);
}

// Auto-detect the IoEngine: try UringEngine first (Linux with liburing),
// fall back to BlockingEngine. Returns nullptr on error.
static std::shared_ptr<crowdb::diskio::IoEngine> create_engine(const crowdb::diskio::DioConfig &cfg)
{
    using namespace crowdb::diskio;
#ifdef CROWDB_HAVE_LIBURING
    try {
        auto engine = std::make_shared<UringEngine>(cfg.sq_entries);
        return engine;
    }
    catch (const std::exception &e) {
        std::fprintf(stderr, "warning: uring engine creation failed (%s), falling back to blocking\n", e.what());
    }
#endif
    return std::make_shared<BlockingEngine>(cfg.thread_pool_size);
}

// Build the DiskSet from config. Disks with a non-empty path are
// BlockDisk (O_DIRECT block device); disks with an empty path are
// dummy disks (NullDisk or MemDisk per config). For UringEngine, all
// disk fds (real block devices + memfd-backed dummy disks) are
// registered with the uring for fd→pipeline routing.
static std::shared_ptr<crowdb::diskio::DiskSet> build_disk_set(const crowdb::diskio::DioConfig          &cfg,
                                                               std::shared_ptr<crowdb::diskio::IoEngine> engine)
{
    using namespace crowdb::diskio;
    auto disk_set = std::make_shared<DiskSet>();
#ifdef CROWDB_HAVE_LIBURING
    auto uring_engine = std::dynamic_pointer_cast<UringEngine>(engine);
#endif
    for (const auto &entry : cfg.disks) {
        std::shared_ptr<Disk> disk;
        if (entry.path.empty()) {
            // Dummy disk (NullDisk or MemDisk).
            auto zones = std::vector<Zone>(entry.zones);
            if (cfg.dummy_disk_type == DummyDiskType::Mem) {
                disk = std::make_shared<MemDisk>(entry.id, engine, std::move(zones), cfg.dummy_props);
            }
            else {
                disk = std::make_shared<NullDisk>(entry.id, engine, std::move(zones), cfg.dummy_props);
            }
        }
        else {
            // Real block device.
            disk =
                std::make_shared<BlockDisk>(entry.id, entry.path, engine, std::vector<Zone>(entry.zones), cfg.o_direct);
        }
#ifdef CROWDB_HAVE_LIBURING
        // Register the disk's fd with the uring for fd→pipeline routing.
        // All disk types (BlockDisk, MemDisk, NullDisk) have real fds that
        // go through the uring submit path; unregistered fds fall back to
        // pipeline 0 but their completions are not reliably delivered.
        if (uring_engine != nullptr && disk->fd() >= 0) {
            uring_engine->uring().register_fd(disk->fd());
        }
#endif
        disk_set->add(disk);
    }
    return disk_set;
}

int main(int argc, char *argv[])
{
    using namespace crowdb::diskio;

    // Disable stdout/stderr buffering so output is immediately visible
    // when captured via pipes/files (e.g. in integration tests).
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    std::setvbuf(stderr, nullptr, _IONBF, 0);

    DioConfig   cfg;
    std::string err;
    if (!DioConfig::parse_args(argc, argv, cfg, err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        return 1;
    }
    if (!cfg.validate(err)) {
        std::fprintf(stderr, "error: %s\n", err.c_str());
        return 1;
    }

    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    // Auto-detect and create the engine.
    auto engine = create_engine(cfg);
    if (engine == nullptr) {
        return 1;
    }

    // Build the disk set (disks share the engine).
    auto disk_set = build_disk_set(cfg, engine);
    if (disk_set == nullptr) {
        return 1;
    }

    // Create + start the RPC server.
    crowdb::rpc::RpcServer server;
    if (!server.listen(cfg.bind_address, cfg.listen_port)) {
        std::fprintf(stderr, "error: failed to listen on %s:%d\n", cfg.bind_address.c_str(), cfg.listen_port);
        return 1;
    }
    int actual_port = server.listen_port();
    std::printf("crowdb-diskio listening on %s:%d (%zu disks)\n", cfg.bind_address.c_str(), actual_port,
                disk_set->size());
    std::fflush(stdout);

    auto *transport  = server.transport();
    auto  dio_server = std::make_unique<DiskioServer>(disk_set, transport);
    dio_server->register_handlers(server);

    server.start();

    // Scheduled executor for periodic tasks (group-0 sync, etc.).
    // The main loop polls run_due_tasks() every ~100ms.
    crowdb::rpc::ScheduledExecutor scheduler;

    // Start group-0 sync if kv_seeds are configured.
    std::unique_ptr<Group0Sync> group0_sync;
    if (!cfg.kv_seeds.empty()) {
        Group0SyncConfig g0_cfg;
        g0_cfg.kv_seeds            = cfg.kv_seeds;
        g0_cfg.instance_id         = cfg.instance_id;
        g0_cfg.rack_id             = cfg.rack_id;
        g0_cfg.node_id             = cfg.node_id;
        g0_cfg.dg_id               = cfg.dg_id;
        g0_cfg.sync_interval_ms    = cfg.sync_interval_ms;
        g0_cfg.rpc_endpoint        = cfg.bind_address + ":" + std::to_string(actual_port);
        g0_cfg.auto_discover_disks = cfg.auto_discover_disks;
        g0_cfg.dummy_disk_type     = cfg.dummy_disk_type;
        g0_cfg.dummy_props         = cfg.dummy_props;
        group0_sync                = std::make_unique<Group0Sync>(std::move(g0_cfg), disk_set, engine, scheduler);
        group0_sync->start();
        std::printf("group-0 sync started (interval=%ums, dg=%llu)\n", cfg.sync_interval_ms,
                    static_cast<unsigned long long>(cfg.dg_id));
    }

    // Start system metrics logging (CPU, RSS, TCP, DRAM BW).
    std::unique_ptr<crowdb::common::metrics::SystemCollector> sys_collector;
    FILE *metrics_fp = nullptr;
    if (cfg.metrics_interval_secs > 0) {
        metrics_fp = open_metrics_log(cfg.metrics_log_dir);
        if (metrics_fp != nullptr) {
            sys_collector = std::make_unique<crowdb::common::metrics::SystemCollector>();
            // Prime the collector with a baseline read.
            (void)sys_collector->collect();
            schedule_metrics_flush(scheduler, *sys_collector, metrics_fp, cfg.metrics_interval_secs);
            std::printf("system metrics started (interval=%us)\n", cfg.metrics_interval_secs);
        }
    }

    // Run until signaled. Poll the scheduler every 100ms.
    while (g_running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        scheduler.run_due_tasks();
    }

    // Final metrics flush.
    if (metrics_fp != nullptr && sys_collector != nullptr) {
        auto snap = sys_collector->collect();
        auto ts   = iso8601_now();
        std::fprintf(metrics_fp, "[%s window=final]\n", ts.c_str());
        std::fprintf(metrics_fp, "misc\n");
        crowdb::common::metrics::flush_system(metrics_fp, snap);
        std::fflush(metrics_fp);
        std::fclose(metrics_fp);
    }

    if (group0_sync != nullptr) {
        group0_sync->stop();
    }
    server.stop();
    disk_set->shutdown();

    // Stop the engine (BlockingEngine has a stop() method).
    if (auto *be = dynamic_cast<BlockingEngine *>(engine.get())) {
        be->stop();
    }

    // Shut down the FFI tokio runtime (if it was initialized).
    if (!cfg.kv_seeds.empty()) {
        crowdb_kv_ffi_shutdown();
    }

    return 0;
}
