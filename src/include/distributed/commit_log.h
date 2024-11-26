//===----------------------------------------------------------------------===//
//
//                         Chfs
//
// commit_log.h
//
// Identification: src/include/distributed/commit_log.h
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "block/manager.h"
#include "common/config.h"
#include "common/macros.h"
#include "filesystem/operations.h"
#include <atomic>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_set>
#include <vector>

namespace chfs {
/**
 * `BlockOperation` is an entry indicates an old block state and
 * a new block state. It's used to redo the operation when
 * the system is crashed.
 */
class BlockOperation {
public:
  explicit BlockOperation(block_id_t block_id, std::vector<u8> new_block_state)
      : block_id_(block_id), new_block_state_(new_block_state) {
    CHFS_ASSERT(new_block_state.size() == DiskBlockSize, "invalid block state");
  }

  block_id_t block_id_;
  std::vector<u8> new_block_state_;
};

class ServerOperation {
public:
  explicit ServerOperation(u8 type_, inode_id_t parent_, std::string name_)
      : type(type_), parent(parent_), name(name_) {
  }
  u8 type; //0 unlink, 1 mknode file, 2 mknode directory
  inode_id_t parent;
  std::string name;
};

/**
 * `CommitLog` is a class that records the block edits into the
 * commit log. It's used to redo the operation when the system
 * is crashed.
 */
class CommitLog {
public:
  explicit CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled);
  ~CommitLog();
  auto append_log(txn_id_t txn_id,
                  std::vector<std::shared_ptr<BlockOperation>> ops) -> void;
  auto commit_log(txn_id_t txn_id) -> void;
  auto checkpoint() -> void;
  auto recover() -> void;
  auto get_log_entry_num() -> usize;

  bool is_checkpoint_enabled_;
  std::shared_ptr<BlockManager> bm_;
  /**
   * {Append anything if you need}
   */
  block_id_t LogBlockCnt = 1024U;
  block_id_t BitmapBytes = 128U;
  block_id_t BytesPerTxEntry = 4U;//2 for LogBlockId, 1 for log count, 1 for Submit
  block_id_t BytesPerLogEntry = 10U;//8 for block_id_t, 2 for LogBlockId
  txn_id_t max_txn_id = 0U;

  std::mutex superblock_lock;
  std::mutex logsystem_lock;
  std::map<txn_id_t, std::mutex>txn_lock;

  bool generating_ckpt;
  int working_jobs = 0;
  int log_entries_num = 0;
  std::mutex FAAjob_lock;
  std::mutex FAAlog_lock;
  auto FAAjob(int val) -> void;
  auto FAAlog(int val) -> void;

  auto find_free_log_block(u8* data) -> block_id_t;
  auto alloc_txn(ServerOperation operation) -> txn_id_t;
  auto get_txn_info(txn_id_t txn_id) -> std::pair<block_id_t,usize>;
  auto alloc_log(txn_id_t txn_id, usize log_cnt) -> std::tuple<block_id_t, usize, std::vector<block_id_t>>;

  auto recover_tx(txn_id_t txn_id, block_id_t txn_block_id, usize log_cnt) -> void;

  std::vector<ServerOperation> recover_operations;
  usize OperationOffset = DiskBlockSize - 100U;
  auto set_operation(block_id_t txn_block_id, ServerOperation operation) -> void;
  auto get_operation(block_id_t txn_block_id) -> ServerOperation;
};

} // namespace chfs