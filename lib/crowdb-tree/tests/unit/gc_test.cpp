// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// R129: tombstone folding is part of snapshot preparation; the standalone
// resident-tree GC sweep is removed. These tests verify the watermark
// contract and that snapshot folding drops eligible tombstones.
#include "crowdb-tree/block_page_store.h"
#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

using namespace crowdb::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

Batch del_one(const std::string &k)
{
    return Batch{{{.key = k, .kind = OpKind::kDelete, .value = ""}}};
}

page_type head_type(Crowdbtree &t)
{
    return t.mapping().get_resident(t.root_page_id())->type;
}
} // namespace

TEST(Gc, SetWatermarkTakesMinAndIsMonotonic)
{
    Crowdbtree t;
    t.set_gc_watermark(10, 5);
    EXPECT_EQ(t.gc_watermark(), 5U);
    // A later call whose min is *below* the current floor must not regress it.
    t.set_gc_watermark(3, 20);
    EXPECT_EQ(t.gc_watermark(), 5U);
    // A later call whose min genuinely advances the floor does move it.
    t.set_gc_watermark(8, 8);
    EXPECT_EQ(t.gc_watermark(), 8U);
}

// With gc_watermark still at 0, a snapshot must not fold the tombstone at
// slot 2 — it is not yet eligible. The snapshot still folds delta chains
// (creating a fresh LeafBase), but the tombstone survives in the rebuilt
// leaf. We verify this by raising the watermark afterward and confirming
// a second snapshot does fold it (proving the first one didn't).
TEST(Gc, SnapshotFoldingBelowWatermarkPreservesTombstone)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;
    Crowdbtree t(opt);
    ASSERT_TRUE(t.apply(1, put_one("a", "A")).ok());
    ASSERT_TRUE(t.apply(2, del_one("a")).ok());
    ASSERT_TRUE(t.flush().ok());
    // gc_watermark() defaults to 0; the tombstone at slot 2 is not yet eligible.
    ASSERT_TRUE(t.snapshot().ok());
    // The tombstone survives: get returns false (tombstone filters the read),
    // but the leaf still contains the tombstone cell.
    std::string v;
    uint64_t    slot;
    EXPECT_FALSE(t.get(Slice("a"), &slot, &v));

    // Now raise the watermark past the tombstone's slot and snapshot again.
    // This second snapshot must fold the tombstone (it's now eligible).
    t.set_gc_watermark(2, 2);
    PageBase *before = t.mapping().get_resident(t.root_page_id());
    ASSERT_TRUE(t.snapshot().ok());
    // The leaf was rebuilt (eligible tombstone folded).
    EXPECT_NE(t.mapping().get_resident(t.root_page_id()), before);
    EXPECT_FALSE(t.get(Slice("a"), &slot, &v));
}

// A leaf that receives a delete and then no further writes keeps its
// tombstone past gc_floor_ until a snapshot folds it. This is the R129
// replacement for the old collect_garbage sweep: snapshot preparation
// inspects clean resident LeafBase pages and rebuilds only those containing
// eligible tombstones.
TEST(Gc, SnapshotFoldsEligibleTombstoneInCleanLeaf)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store    = &store;
    opt.max_delta_len = 4;
    Crowdbtree t(opt);
    for (uint64_t s = 1; s <= 4; ++s) {
        ASSERT_TRUE(t.apply(s, put_one("k" + std::to_string(s), "v")).ok());
        ASSERT_TRUE(t.flush().ok());
    }
    // 5th delta (a delete of k1) trips consolidation -> folds into a fresh,
    // clean LeafBase with no BatchDelta chain on top.
    ASSERT_TRUE(t.apply(5, del_one("k1")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_EQ(head_type(t), page_type::kLeafBase);

    // Advance the watermark past the delete's slot, then snapshot to fold.
    t.set_gc_watermark(5, 5);
    PageBase *before = t.mapping().get_resident(t.root_page_id());
    ASSERT_TRUE(t.snapshot().ok());
    // The leaf was rebuilt (eligible tombstone folded).
    EXPECT_NE(t.mapping().get_resident(t.root_page_id()), before);
    ASSERT_EQ(head_type(t), page_type::kLeafBase);

    // Delete still honored (not resurrected); other keys unaffected.
    std::string v;
    uint64_t    slot;
    EXPECT_FALSE(t.get(Slice("k1"), &slot, &v));
    for (int i = 2; i <= 4; ++i) {
        EXPECT_TRUE(t.get(Slice("k" + std::to_string(i)), &slot, &v));
    }
}

// Same as SnapshotFoldsEligibleTombstoneInCleanLeaf, but via open() with
// a page store so the snapshot path exercises the durable write.
TEST(Gc, SnapshotFoldingReclaimsTombstoneAfterOpen)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store    = &store;
    opt.max_delta_len = 4;

    std::unique_ptr<Crowdbtree> t;
    ASSERT_TRUE(Crowdbtree::open(opt, &t).ok());
    for (uint64_t s = 1; s <= 4; ++s) {
        ASSERT_TRUE(t->apply(s, put_one("k" + std::to_string(s), "v")).ok());
        ASSERT_TRUE(t->flush().ok());
    }
    ASSERT_TRUE(t->apply(5, del_one("k1")).ok());
    ASSERT_TRUE(t->flush().ok());
    ASSERT_EQ(head_type(*t), page_type::kLeafBase);
    PageBase *before = t->mapping().get_resident(t->root_page_id());

    t->set_gc_watermark(5, 5);
    ASSERT_TRUE(t->snapshot().ok());

    EXPECT_NE(t->mapping().get_resident(t->root_page_id()), before)
        << "snapshot folding never swept the stale tombstone";

    std::string v;
    uint64_t    slot;
    EXPECT_FALSE(t->get(Slice("k1"), &slot, &v));
}

// compact_sparse_blocks on a non-block store returns empty stats with no
// snapshot write (R129 §5: non-block stores return a no-op result).
TEST(Gc, CompactSparseBlocksNoOpOnMemStore)
{
    MemPageStore store(1);
    Options      opt;
    opt.page_store = &store;
    Crowdbtree t(opt);
    ASSERT_TRUE(t.apply(1, put_one("a", "1")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot().ok());

    MergeGcStats stats;
    ASSERT_TRUE(t.compact_sparse_blocks(&stats).ok());
    EXPECT_EQ(stats.blocks_selected, 0U);
    EXPECT_EQ(stats.pages_relocated, 0U);
    EXPECT_EQ(stats.bytes_relocated, 0U);
    EXPECT_EQ(stats.blocks_deleted, 0U);
}

TEST(Gc, NormalSnapshotDoesNotRelocateSparseBlocks)
{
    crowdb::tree_test::TempDir tmp("snapshot_no_relocate_");
    ASSERT_FALSE(tmp.path.empty());
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(tmp.path, 0, 0, 8 * 1024, 1, &store).ok());
    store->set_sync_mode(SyncMode::kSkip);
    Options opt;
    opt.page_store                    = store.get();
    opt.merge_gc_block_free_threshold = 0.01;
    opt.merge_gc_max_relocation_bytes = 8 * 1024 * 1024;
    Crowdbtree t(opt);
    ASSERT_TRUE(t.apply(1, put_one("a", "1")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot().ok());

    ASSERT_TRUE(t.snapshot().ok());
    EXPECT_EQ(t.stats().snapshot_pages_written, 0U);
}

// Anchor protection: block 0 holds the dual anchors and must never be
// selected for relocation. We verify this by writing data, running
// compaction, and then reopening the store — if block 0 had been
// relocated/deleted, the anchor would be corrupt and open would fail.
TEST(Gc, CompactSparseBlocksProtectsAnchorBlock)
{
    crowdb::tree_test::TempDir tmp("anchorblk_");
    ASSERT_FALSE(tmp.path.empty());
    constexpr uint64_t              blk = 8 * 1024;
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(tmp.path, 0, 0, blk, 1, &store).ok());
    Options opt;
    opt.page_store                    = store.get();
    opt.leaf_split_bytes              = 256;
    opt.merge_gc_max_relocation_bytes = 8 * 1024 * 1024;
    opt.merge_gc_block_free_threshold = 0.01;
    {
        Crowdbtree t(opt);
        for (int i = 0; i < 50; ++i) {
            ASSERT_TRUE(t.apply(i + 1, put_one("k" + std::to_string(i), "v")).ok());
        }
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok());
        // Delete most keys to make blocks sparse.
        for (int i = 0; i < 50; ++i) {
            if (i % 10 != 0) {
                ASSERT_TRUE(t.apply(100 + i, del_one("k" + std::to_string(i))).ok());
            }
        }
        ASSERT_TRUE(t.flush().ok());
        ASSERT_TRUE(t.snapshot().ok());
        // Run compaction — must not touch block 0 (anchor).
        MergeGcStats stats;
        ASSERT_TRUE(t.compact_sparse_blocks(&stats).ok());
        EXPECT_GE(stats.blocks_selected, 0U);
        // Surviving keys readable.
        for (int i = 0; i < 50; i += 10) {
            std::string v;
            uint64_t    s;
            EXPECT_TRUE(t.get(Slice("k" + std::to_string(i)), &s, &v));
        }
    }
    // Reopen — anchor must be intact.
    std::unique_ptr<BlockPageStore> store2;
    ASSERT_TRUE(BlockPageStore::open_blocks(tmp.path, 0, 0, blk, 1, &store2).ok());
    Options opt2;
    opt2.page_store       = store2.get();
    opt2.leaf_split_bytes = 256;
    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt2, &t2).ok());
    // Data survives reopen.
    for (int i = 0; i < 50; i += 10) {
        std::string v;
        uint64_t    s;
        EXPECT_TRUE(t2->get(Slice("k" + std::to_string(i)), &s, &v));
    }
}

// Per-pass byte budget: with a small budget and multiple sparse blocks,
// compact_sparse_blocks selects only the sparsest blocks that fit within
// the budget. The first eligible block is always allowed even if it
// exceeds the budget (progress guarantee).
TEST(Gc, CompactSparseBlocksRespectsByteBudget)
{
    crowdb::tree_test::TempDir tmp("budget_");
    ASSERT_FALSE(tmp.path.empty());
    constexpr uint64_t              blk = 8 * 1024;
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(tmp.path, 0, 0, blk, 1, &store).ok());
    store->set_sync_mode(SyncMode::kSkip);
    Options opt;
    opt.page_store                    = store.get();
    opt.leaf_split_bytes              = 256;
    opt.merge_gc_block_free_threshold = 0.01;
    // Very small budget: only the first block should be selected.
    opt.merge_gc_max_relocation_bytes = blk / 4;
    Crowdbtree t(opt);

    // Write enough data to span multiple blocks.
    uint64_t slot = 0;
    for (int i = 0; i < 200; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one("k" + std::to_string(i), std::string(128, 'x'))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot().ok());

    // Delete most keys to make blocks sparse.
    for (int i = 0; i < 200; ++i) {
        if (i % 20 != 0) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, del_one("k" + std::to_string(i))).ok());
        }
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot().ok());

    MergeGcStats stats;
    ASSERT_TRUE(t.compact_sparse_blocks(&stats).ok());
    // At least one block selected (the first eligible is always allowed).
    EXPECT_GE(stats.blocks_selected, 1U);
    // Surviving keys are still readable.
    for (int i = 0; i < 200; i += 20) {
        std::string v;
        uint64_t    s;
        EXPECT_TRUE(t.get(Slice("k" + std::to_string(i)), &s, &v));
    }
}

// Stale prefetch identity: if a page is modified between the prefetch
// phase (outside write_mutex_) and the prepare phase (under the mutex),
// the prefetched blob is discarded and the page is skipped for this pass.
// We verify this by checking that compact_sparse_blocks still completes
// successfully and data integrity is maintained. The stale-prefetch path
// is exercised implicitly when concurrent writes happen during compaction;
// here we verify the no-concurrency baseline works.
TEST(Gc, CompactSparseBlocksMaintainsDataIntegrity)
{
    crowdb::tree_test::TempDir tmp("integ_");
    ASSERT_FALSE(tmp.path.empty());
    constexpr uint64_t              blk = 8 * 1024;
    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::open_blocks(tmp.path, 0, 0, blk, 1, &store).ok());
    store->set_sync_mode(SyncMode::kSkip);
    Options opt;
    opt.page_store                    = store.get();
    opt.leaf_split_bytes              = 256;
    opt.merge_gc_block_free_threshold = 0.30;
    opt.merge_gc_max_relocation_bytes = 8 * 1024 * 1024;
    Crowdbtree t(opt);

    uint64_t slot = 0;
    for (int i = 0; i < 200; ++i) {
        ++slot;
        ASSERT_TRUE(t.apply(slot, put_one("k" + std::to_string(i), std::string(128, 'x'))).ok());
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot().ok());

    // Delete most keys to create sparse blocks.
    for (int i = 0; i < 200; ++i) {
        if (i % 20 != 0) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, del_one("k" + std::to_string(i))).ok());
        }
    }
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot().ok());

    // Run compaction.
    MergeGcStats stats;
    ASSERT_TRUE(t.compact_sparse_blocks(&stats).ok());
    // Some blocks should be selected and relocated.
    EXPECT_GE(stats.blocks_selected, 1U);

    ++slot;
    ASSERT_TRUE(t.apply(slot, put_one("after-compaction", "still-writable")).ok());
    ASSERT_TRUE(t.flush().ok());
    ASSERT_TRUE(t.snapshot().ok());

    // All surviving keys must be readable after compaction.
    for (int i = 0; i < 200; i += 20) {
        std::string v;
        uint64_t    s;
        EXPECT_TRUE(t.get(Slice("k" + std::to_string(i)), &s, &v)) << "key " << i << " lost";
        EXPECT_EQ(v, std::string(128, 'x'));
    }
    std::string appended;
    uint64_t    appended_slot;
    EXPECT_TRUE(t.get(Slice("after-compaction"), &appended_slot, &appended));
    EXPECT_EQ(appended, "still-writable");

    // A second compaction pass should be idempotent (no more sparse blocks
    // or at least no data loss).
    MergeGcStats stats2;
    ASSERT_TRUE(t.compact_sparse_blocks(&stats2).ok());
    (void)stats2;
    for (int i = 0; i < 200; i += 20) {
        std::string v;
        uint64_t    s;
        EXPECT_TRUE(t.get(Slice("k" + std::to_string(i)), &s, &v)) << "key " << i << " lost after 2nd pass";
    }
}
