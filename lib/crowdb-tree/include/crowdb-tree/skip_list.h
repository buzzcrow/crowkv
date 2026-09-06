// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// ConcurrentSkipList: a lock-free-read, single-writer ordered map for
// the epoch-protected MemTable (R50). Keys are byte slices stored inline
// in each node (RocksDB InlineSkipList style); values are CellVersion
// pointers atomically swapped on overwrite so a reader under an epoch
// guard can borrow the old version safely while the writer retires it.
//
// Writers are serialized by an internal spinlock (apply() already
// serializes today; CAS-based concurrent insert is out of scope). Readers
// traverse the next[] tower with acquire loads — no lock, no copy. Erase
// is a logical tombstone + unlink + epoch-deferred reclamation; a cursor
// already positioned on an unlinked node can still read it and advance
// via its next[0] (which still points forward), skipping deleted nodes.
//
// The list does NOT own reclamation — the caller (MemTable) retires nodes
// and cell versions through the engine's EpochManager. This keeps one EBR
// instance covering both L0 nodes and L1 pages.
#pragma once

#include "crowdb-tree/buffer.h"
#include "crowdb-tree/cell.h"
#include "crowdb-tree/slice.h"

#include <atomic>
#include <cstdint>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace crowdb::tree
{

// A versioned cell value held by a skip-list node. Allocated separately so
// it can be atomically swapped (overwrite) and epoch-retired independently
// of the node. The buffer is the same contiguous-or-split form as
// cell_entry::cell (R30): kOwned = full [header][value]; kExternal =
// value-only borrowed from a Rust Bytes (drop_fn fires on destruction).
struct CellVersion
{
    buffer   cell;
    uint64_t slot;
    uint8_t  flags;
};

// Skip-list node with inline key. Layout: fixed fields, then the
// next_[height] tower, then the key bytes. Allocated via operator new
// with the exact size. The key bytes are immutable for the node's
// lifetime; only the cell version pointer is mutable (atomic release
// store on overwrite, acquire load on read).
struct Node
{
    std::atomic<CellVersion *> cell_{nullptr};  // current cell version
    std::atomic<bool>          deleted_{false}; // logical tombstone
    uint32_t                   height_{1};      // tower height
    uint32_t                   key_len_{0};     // inline key length

    // The tower follows the fixed fields. Access via next_ptr(level).
    [[nodiscard]] std::atomic<Node *> *next_ptr(uint32_t level)
    {
        return reinterpret_cast<std::atomic<Node *> *>(reinterpret_cast<char *>(this) + sizeof(Node)) + level;
    }

    [[nodiscard]] const std::atomic<Node *> *next_ptr(uint32_t level) const
    {
        return reinterpret_cast<const std::atomic<Node *> *>(reinterpret_cast<const char *>(this) + sizeof(Node)) +
               level;
    }

    [[nodiscard]] Node *next(uint32_t level) const
    {
        return next_ptr(level)->load(std::memory_order_acquire);
    }

    void set_next(uint32_t level, Node *n)
    {
        next_ptr(level)->store(n, std::memory_order_release);
    }

    [[nodiscard]] const char *key_data() const
    {
        return reinterpret_cast<const char *>(this) + sizeof(Node) + (sizeof(std::atomic<Node *>) * height_);
    }

    [[nodiscard]] Slice key_slice() const
    {
        return {key_data(), key_len_};
    }

    // Total allocation size for a node with `height` and `key_len`.
    [[nodiscard]] static size_t alloc_size(uint32_t height, size_t key_len)
    {
        return sizeof(Node) + (sizeof(std::atomic<Node *>) * height) + key_len;
    }
};

class ConcurrentSkipList
{
  public:
    static constexpr uint32_t kMaxHeight = 12;

    // RAII cursor for lock-free ordered iteration. Seeded by a lower_bound
    // seek; advance() skips logically-deleted nodes. A cursor may be
    // positioned on a node that has been unlinked — the node's key/cell
    // remain readable (epoch guard keeps it alive) and next[0] still points
    // forward, so advance() continues correctly.
    class Cursor
    {
      public:
        Cursor() = default;

        explicit Cursor(const Node *n) : cur_(n)
        {
        }

        [[nodiscard]] bool valid() const
        {
            return cur_ != nullptr;
        }

        [[nodiscard]] Slice key() const
        {
            return cur_->key_slice();
        }

        [[nodiscard]] const CellVersion *cell_version() const
        {
            return cur_->cell_.load(std::memory_order_acquire);
        }

        // Advance to the next live (non-deleted) node. Skips deleted nodes
        // by following next[0] until a live one is found or the tail is
        // reached.
        void advance();

        // Prefetch the next node's memory (the one advance() will move to).
        // A non-faulting hint — brings the next node into CPU cache before
        // the merge loop calls advance(), overlapping the cache fill with
        // the current merge step's work.
        void prefetch_next() const
        {
            if (cur_ != nullptr) {
                if (const Node *n = cur_->next(0); n != nullptr) {
                    __builtin_prefetch(n, 0, 1);
                }
            }
        }

      private:
        const Node *cur_ = nullptr;
    };

    ConcurrentSkipList();
    ~ConcurrentSkipList();

    ConcurrentSkipList(const ConcurrentSkipList &)            = delete;
    ConcurrentSkipList &operator=(const ConcurrentSkipList &) = delete;

    // Insert a new key or overwrite an existing key's cell version.
    // Returns true if accepted (list takes ownership of `cv`; `*out_old`
    // is the previous version on overwrite, nullptr on new insert — caller
    // retires it via epoch). Returns false if rejected (existing entry has
    // a >= slot; caller retains ownership of `cv`; `*out_old` is nullptr).
    bool upsert(Slice key, CellVersion *cv, CellVersion **out_old);

    // Point lookup: returns the CellVersion* for `key`, or nullptr. The
    // returned pointer is valid only while the caller's epoch guard is held
    // (a concurrent overwrite retires the old version via epoch).
    [[nodiscard]] const CellVersion *find(Slice key) const;

    // Return a cursor positioned at the first live node with key >
    // `start_after` (or the first live node if start_after is empty).
    [[nodiscard]] Cursor cursor(Slice start_after) const;

    // Remove and return all live entries with slot <= `cs`, in key order.
    // Each removed entry's node is unlinked and returned (caller retires the
    // node and its CellVersion via epoch). The key is copied into the
    // returned entry (the node's inline key is freed at reclamation).
    struct DrainedEntry
    {
        std::string  key;
        CellVersion *cv;
        Node        *node; // unlinked node — caller retires via epoch
        uint64_t     slot;
    };

    std::vector<DrainedEntry> drain_up_to(uint64_t cs);

    // Remove and return ALL live entries (for reset()). Same retirement
    // contract as drain_up_to.
    std::vector<DrainedEntry> drain_all();

    [[nodiscard]] size_t count() const
    {
        return count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool empty() const
    {
        return count_.load(std::memory_order_relaxed) == 0;
    }

    [[nodiscard]] size_t approx_bytes() const
    {
        return bytes_.load(std::memory_order_relaxed);
    }

    void add_bytes(size_t n)
    {
        bytes_.fetch_add(n, std::memory_order_relaxed);
    }

    void sub_bytes(size_t n)
    {
        bytes_.fetch_sub(n, std::memory_order_relaxed);
    }

    // Public so MemTable can pass it to epoch_.retire() as the deleter.
    static void free_node(void *p);

  private:
    friend class Cursor;

    // RAII spinlock guard for writer serialization.
    struct SpinlockGuard
    {
        std::atomic<bool> &lock;

        explicit SpinlockGuard(std::atomic<bool> &l) : lock(l)
        {
            uint32_t attempts = 0;
            while (lock.exchange(true, std::memory_order_acquire)) {
                if (++attempts < 16) {
#if defined(__x86_64__) || defined(_M_X64)
                    __builtin_ia32_pause();
#elif defined(__aarch64__)
                    asm volatile("yield");
#endif
                }
                else {
                    std::this_thread::yield();
                    attempts = 0;
                }
            }
        }

        ~SpinlockGuard()
        {
            lock.store(false, std::memory_order_release);
        }

        SpinlockGuard(const SpinlockGuard &)            = delete;
        SpinlockGuard &operator=(const SpinlockGuard &) = delete;
    };

    [[nodiscard]] static Node *alloc_node(uint32_t height, Slice key);

    uint32_t random_height();

    // Find the predecessors of the first node with key >= `key` at each
    // level. Fills `prev` (size kMaxHeight). Returns the node at level 0
    // with key >= `key` (or nullptr if none). Must be called under
    // spinlock_ for write paths; readers use find()/cursor() without it.
    Node *find_ge(Slice key, Node **prev) const;

    Node                 *head_;
    std::atomic<uint32_t> max_height_{1};
    std::atomic<bool>     spinlock_{false};
    std::atomic<size_t>   count_{0};
    std::atomic<size_t>   bytes_{0};
    std::mt19937          rng_;
};

} // namespace crowdb::tree
