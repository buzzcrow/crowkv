// Copyright 2026-present Gian <crow.db@outlook.com>

#include "crowdb-common/metrics/system_metrics.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <string>

using crowdb::common::metrics::flush_system;
using crowdb::common::metrics::SystemCollector;
using crowdb::common::metrics::SystemMetricsSnapshot;

TEST(SystemMetricsTest, CollectReturnsSnapshot)
{
    SystemCollector collector;
    // First collect sets baselines; second gives real deltas.
    SystemMetricsSnapshot snap = collector.collect();
    // RSS should be non-zero (the test process has memory).
    EXPECT_GT(snap.rss_kb, 0U);
}

TEST(SystemMetricsTest, CollectDeltaIsNonNegative)
{
    SystemCollector collector;
    (void)collector.collect();
    SystemMetricsSnapshot snap = collector.collect();
    // CPU utilization can exceed 100% on multi-core.
    EXPECT_LE(snap.cpu_user_pct, 100U * 1024U);
}

TEST(SystemMetricsTest, FlushWritesAllFields)
{
    SystemMetricsSnapshot snap;
    snap.cpu_user_pct    = 42;
    snap.cpu_sys_pct     = 17;
    snap.rss_kb          = 4096;
    snap.tcp_retransmits = 3;
    snap.tcp_lost        = 1;
    snap.dram_bw_mib     = 512.5;

    // Write to a temp file via FILE*.
    FILE *fp = std::tmpfile();
    ASSERT_NE(fp, nullptr);
    flush_system(fp, snap);
    std::fseek(fp, 0, SEEK_SET);
    std::array<char, 4096> buf{};
    size_t                 n = std::fread(buf.data(), 1, buf.size() - 1, fp);
    buf[n]                   = '\0';
    std::fclose(fp);

    std::string out(buf.data());
    EXPECT_TRUE(out.find("cpu.user=42%") != std::string::npos);
    EXPECT_TRUE(out.find("cpu.sys=17%") != std::string::npos);
    EXPECT_TRUE(out.find("rss_gb=0.00") != std::string::npos);
    EXPECT_TRUE(out.find("tcp_retrans=3") != std::string::npos);
    EXPECT_TRUE(out.find("tcp_lost=1") != std::string::npos);
    EXPECT_TRUE(out.find("bw_mib=512.5") != std::string::npos);
}

TEST(SystemMetricsTest, FlushWritesUnsupportedWhenNone)
{
    SystemMetricsSnapshot snap;
    snap.dram_bw_mib = std::nullopt;

    FILE *fp = std::tmpfile();
    ASSERT_NE(fp, nullptr);
    flush_system(fp, snap);
    std::fseek(fp, 0, SEEK_SET);
    std::array<char, 1024> buf{};
    size_t                 n = std::fread(buf.data(), 1, buf.size() - 1, fp);
    buf[n]                   = '\0';
    std::fclose(fp);

    std::string out(buf.data());
    EXPECT_TRUE(out.find("bw_mib=unsupported") != std::string::npos);
}
