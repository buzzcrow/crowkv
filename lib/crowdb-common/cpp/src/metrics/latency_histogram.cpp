// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/metrics/latency_histogram.h"

#include <array>
#include <cstdint>
#include <utility>

namespace crowdb::common::metrics
{

static constexpr size_t                        kNumBuckets     = 22;
static const std::array<uint64_t, kNumBuckets> kBucketBoundsNs = {
    1'000,         // 1us
    2'000,         // 2us
    5'000,         // 5us
    10'000,        // 10us
    20'000,        // 20us
    50'000,        // 50us
    100'000,       // 100us
    200'000,       // 200us
    500'000,       // 500us
    1'000'000,     // 1ms
    2'000'000,     // 2ms
    5'000'000,     // 5ms
    10'000'000,    // 10ms
    20'000'000,    // 20ms
    50'000'000,    // 50ms
    100'000'000,   // 100ms
    200'000'000,   // 200ms
    500'000'000,   // 500ms
    1'000'000'000, // 1s
    2'000'000'000, // 2s
    5'000'000'000, // 5s
    UINT64_MAX     // infinity
};

LatencyHistogram::LatencyHistogram(std::string name) : name_(std::move(name)), count_(0), sum_(0), total_count_(0)
{
    buckets_.reserve(kNumBuckets);
    for (size_t i = 0; i < kNumBuckets; ++i) {
        buckets_.push_back(std::make_unique<std::atomic<uint64_t>>(0));
    }
}

void LatencyHistogram::observe(uint64_t ns)
{
    size_t lo = 0;
    size_t hi = kNumBuckets;
    while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        if (kBucketBoundsNs[mid] < ns) {
            lo = mid + 1;
        }
        else {
            hi = mid;
        }
    }
    if (lo >= kNumBuckets) {
        lo = kNumBuckets - 1;
    }
    buckets_[lo]->fetch_add(1, std::memory_order_relaxed);
    count_.fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(ns, std::memory_order_relaxed);
    total_count_.fetch_add(1, std::memory_order_relaxed);
}

LatencyHistogram::Snapshot LatencyHistogram::flush()
{
    Snapshot snap;
    snap.count       = count_.exchange(0, std::memory_order_relaxed);
    snap.sum         = sum_.exchange(0, std::memory_order_relaxed);
    snap.total_count = total_count_.load(std::memory_order_relaxed);
    snap.bucket_counts.resize(kNumBuckets);
    for (size_t i = 0; i < kNumBuckets; ++i) {
        snap.bucket_counts[i] = buckets_[i]->exchange(0, std::memory_order_relaxed);
    }
    return snap;
}

uint64_t LatencyHistogram::percentile(const Snapshot &snap, double p)
{
    if (snap.count == 0) {
        return 0;
    }
    double target_d = static_cast<double>(snap.count) * p / 100.0;
    auto   target   = static_cast<uint64_t>(target_d);
    if (target == 0) {
        target = 1;
    }
    uint64_t cumulative = 0;
    for (size_t i = 0; i < kNumBuckets; ++i) {
        cumulative += snap.bucket_counts[i];
        if (cumulative >= target) {
            return kBucketBoundsNs[i];
        }
    }
    return kBucketBoundsNs[kNumBuckets - 1];
}

} // namespace crowdb::common::metrics
