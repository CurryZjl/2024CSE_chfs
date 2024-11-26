#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  return usize(log_entries_num);
}

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  while(generating_ckpt){}
  FAAjob(1);
  txn_lock[txn_id].lock();
  auto alloc_res = alloc_log(txn_id, ops.size());
  block_id_t txn_block_id = std::get<0>(alloc_res) + this->bm_->total_blocks();
  usize log_idx = std::get<1>(alloc_res);
  usize cur_op_idx = 0U;
  std::vector<block_id_t> log_blocks = std::get<2>(alloc_res);
  std::vector<u8> txn_buffer(DiskBlockSize);
  this->bm_->read_block(txn_block_id, txn_buffer.data());
  for(const auto &op : ops){
    block_id_t log_offset = (log_idx + cur_op_idx) * BytesPerLogEntry;
    block_id_t op_block_id = op->block_id_;
    block_id_t tmp = op_block_id;
    for(int i = 0; i < 8; i++){
      txn_buffer[log_offset + i] = tmp % 256;
      tmp /= 256;
    }
    txn_buffer[log_offset + 8] = log_blocks[cur_op_idx] % 256;
    txn_buffer[log_offset + 9] = log_blocks[cur_op_idx] / 256;
    //save the state block
    std::vector<u8> op_block_state = op->new_block_state_;
    block_id_t log_block_id = log_blocks[cur_op_idx] + this->bm_->total_blocks();
    this->bm_->write_block(log_block_id, op_block_state.data());
    this->bm_->sync(log_block_id);
    cur_op_idx++;
  }
  this->bm_->write_block(txn_block_id,txn_buffer.data());
  this->bm_->sync(txn_block_id);
  txn_lock[txn_id].unlock();
  FAAjob(-1);
  if(log_entries_num >= kMaxLogSize && is_checkpoint_enabled_)
    checkpoint();
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  while(generating_ckpt){}
  FAAjob(1);
  superblock_lock.lock();

  std::vector<u8> buffer(DiskBlockSize);
  block_id_t super_block_id = this->bm_->total_blocks();
  this->bm_->read_block(super_block_id, buffer.data());
  block_id_t offset = BitmapBytes + txn_id * BytesPerTxEntry;
  buffer[offset + 3] = 1;
  this->bm_->write_block(super_block_id, buffer.data());
  this->bm_->sync(super_block_id);

  superblock_lock.unlock();
  FAAjob(-1);
  return;
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  logsystem_lock.lock();
  generating_ckpt = true;
  while(working_jobs > 0){}

  this->bm_->flush();
  for(int i = 0; i < LogBlockCnt; i++)
  {
    block_id_t block_id = this->bm_->total_blocks() + i;
    std::vector<u8> buffer(DiskBlockSize);
    memset(buffer.data(),0,buffer.size());
    this->bm_->write_block(block_id, buffer.data());
    this->bm_->sync(block_id);
  }
  this->max_txn_id = 0;
  FAAlog(-log_entries_num);

  generating_ckpt = false;
  logsystem_lock.unlock();
}

// {Your code here}
auto CommitLog::recover() -> void {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  try{
    logsystem_lock.lock();

    recover_operations.clear();
    //read the superblock
    std::vector<u8> buffer(DiskBlockSize);
    block_id_t LogSuperBlockId = this->bm_->total_blocks();
    this->bm_->read_block(LogSuperBlockId, buffer.data());

    //get the max_txn_id
    block_id_t cur_txn_id = 0;
    while(1)
    {
      block_id_t start_offset = BitmapBytes + cur_txn_id * BytesPerTxEntry;
      if(start_offset + 2 >= DiskBlockSize){
        break;
      }
      block_id_t log_block_id = 0U;
      usize log_count = 0U;
      log_block_id = buffer[start_offset + 0];
      log_block_id *= 256;
      log_block_id += buffer[start_offset + 1];
      log_count = buffer[start_offset + 2];
      if(!log_block_id)
        break;
      cur_txn_id++;
      usize hasCommmit = buffer[start_offset + 2];
      if(!hasCommmit){
        recover_operations.push_back(get_operation(log_block_id));
        continue;
      }
      recover_tx(cur_txn_id-1, log_block_id, log_count);
    }
    max_txn_id = cur_txn_id;

    logsystem_lock.unlock();
  }
  catch(std::exception &e){
    std::cout << e.what()  << std::endl;
  }
}

auto CommitLog::FAAjob(int val) -> void {
  FAAjob_lock.lock();
  working_jobs += val;
  FAAjob_lock.unlock();
}
auto CommitLog::FAAlog(int val) -> void{
  FAAlog_lock.lock();
  log_entries_num += val;
  FAAlog_lock.unlock();
}

auto CommitLog::find_free_log_block(u8* data) -> block_id_t{
  block_id_t free_logblock_id = 0;
  for(int i = 0; i < BitmapBytes; i++){
    u8 cur_map = data[i];
    u8 free_idx = 8U;
    for(int j = 7; j >= 0; j--){
      if(i == 0 && j == 7){
        continue;
      }
      if(!(cur_map & (1 << j))){
        free_idx = j;
        break;
      }
    }
    if(free_idx == 8U)
      continue;
    cur_map |= (1 << free_idx);
    data[i] = cur_map;
    free_logblock_id = i * 8 + (7 - free_idx);
    if(!free_logblock_id)
      continue;
    break;
  }
  CHFS_ASSERT(free_logblock_id > 0,"Cannot find new free Log Block!");
  return free_logblock_id;
}
auto CommitLog::alloc_txn(ServerOperation operation) -> txn_id_t{
  while(generating_ckpt){}

  superblock_lock.lock();
  FAAjob(1);
  //read data from superblock
  std::vector<u8>buffer(DiskBlockSize);
  block_id_t LogSuperBlockId = this->bm_->total_blocks();
  this->bm_->read_block(LogSuperBlockId,buffer.data());

  auto free_logblock_id = find_free_log_block(buffer.data());

  //increment the txn_id
  txn_id_t txn_id = max_txn_id;
  txn_lock[txn_id].lock();
  max_txn_id++;

  //initialize the entry of txn_id
  block_id_t start_offset = BitmapBytes + txn_id * BytesPerTxEntry;
  buffer[start_offset + 0] = (free_logblock_id / 256U);
  buffer[start_offset + 1] = (free_logblock_id % 256U);
  buffer[start_offset + 2] = 0;
  // buffer[start_offset + 3] = 0;

  //write back the superblock
  this->bm_->write_block(LogSuperBlockId,buffer.data());
  this->bm_->sync(LogSuperBlockId);

  //set the operation
  set_operation(free_logblock_id, operation);

  //return
  txn_lock[txn_id].unlock();
  superblock_lock.unlock();
  FAAjob(-1);
  return txn_id;
}
auto CommitLog::get_txn_info(txn_id_t txn_id) -> std::pair<block_id_t,usize>{
  std::vector<u8>buffer(DiskBlockSize);
  block_id_t LogSuperBlockId = this->bm_->total_blocks();
  this->bm_->read_block(LogSuperBlockId,buffer.data());

  block_id_t start_offset = BitmapBytes + txn_id * BytesPerTxEntry;
  block_id_t log_block_id = 0U;
  usize log_count = 0U;
  log_block_id = buffer[start_offset + 0];
  log_block_id *= 256;
  log_block_id += buffer[start_offset + 1];
  log_count = buffer[start_offset + 2];

  return std::make_pair(log_block_id, log_count);
}
auto CommitLog::alloc_log(txn_id_t txn_id, usize log_cnt) -> std::tuple<block_id_t, usize, std::vector<block_id_t>> {
   superblock_lock.lock();

  //read data from superblock
  std::vector<u8>buffer(DiskBlockSize);
  block_id_t LogSuperBlockId = this->bm_->total_blocks();
  this->bm_->read_block(LogSuperBlockId,buffer.data());

  std::vector<block_id_t> free_logblocks;
  free_logblocks.clear();
  for(int i = 0; i < log_cnt; i++)
  {
    auto free_logblock_id = find_free_log_block(buffer.data());
    free_logblocks.push_back(free_logblock_id);
  }


  block_id_t start_offset = BitmapBytes + txn_id * BytesPerTxEntry;
  block_id_t log_block_id = 0U;
  usize log_count = 0U;

  log_block_id = buffer[start_offset + 0];
  log_block_id *= 256;
  log_block_id += buffer[start_offset + 1];

  log_count = buffer[start_offset + 2];
  buffer[start_offset + 2]+=log_cnt;


  //write back the superblock
  this->bm_->write_block(LogSuperBlockId,buffer.data());
  this->bm_->sync(LogSuperBlockId);

  //return
  superblock_lock.unlock();
  FAAlog(log_cnt);
  return std::make_tuple(log_block_id, log_count, free_logblocks);
}

auto CommitLog::recover_tx(txn_id_t txn_id, block_id_t txn_block_id, usize log_cnt) -> void {
  std::vector<u8>buffer (DiskBlockSize);
  this->bm_->read_block(txn_block_id + this->bm_->total_blocks(), buffer.data());
  for(int i = 0; i < log_cnt; i++)
  {
    block_id_t log_offset = i * BytesPerLogEntry;
    block_id_t op_block_id = 0, log_block_id = 0;

    for(int i = 7; i >= 0; i--)
    {
      op_block_id *= 256;
      op_block_id += (block_id_t)buffer[log_offset + i];
    }
    log_block_id = buffer[log_offset + 9], log_block_id *= 256;
    log_block_id = buffer[log_offset + 8];
    log_block_id += this->bm_->total_blocks();
    std::vector<u8>state_data(DiskBlockSize);
    state_data.clear();
    this->bm_->read_block(log_block_id, state_data.data());
    this->bm_->write_block(op_block_id, state_data.data());
  }
}

auto CommitLog::set_operation(block_id_t txn_block_id, ServerOperation operation) -> void {
  txn_block_id += this->bm_->total_blocks();
  std::vector<u8>buffer(DiskBlockSize);
  memset(buffer.data(),0,buffer.size());
  usize cur_byte_pos = OperationOffset;

  buffer[cur_byte_pos] = operation.type;
  cur_byte_pos ++;

  inode_id_t tmp = operation.parent;
  for(int i = 0; i < 8; i++){
    buffer[cur_byte_pos] = tmp % 256, tmp /= 256;
    cur_byte_pos ++;
  }

  size_t op_name_length = operation.name.length();
  for(int i = 0; i < op_name_length; i++){
    buffer[cur_byte_pos] = operation.name[i];
    cur_byte_pos ++;
  }

  buffer[cur_byte_pos] = '\0';

  this->bm_->write_block(txn_block_id, buffer.data());
}

auto CommitLog::get_operation(block_id_t txn_block_id) -> ServerOperation{
  u8 type_ = 0;
  inode_id_t parent_ = 0;
  std::string name_ = "";
  txn_block_id += this->bm_->total_blocks();
  std::vector<u8> buffer(DiskBlockSize);
  this->bm_->read_block(txn_block_id, buffer.data());
  usize cur_byte_pos = OperationOffset;

  type_ = buffer[cur_byte_pos];
  cur_byte_pos++;

  for(int i = 7; i >= 0; i--){
    parent_ *= 256;
    parent_ += buffer[cur_byte_pos + i];
  }
  cur_byte_pos += 8;

  while(buffer[cur_byte_pos])
  {
    name_ += buffer[cur_byte_pos];
    cur_byte_pos++;
  }

  return ServerOperation(type_, parent_, name_);
}
}; // namespace chfs