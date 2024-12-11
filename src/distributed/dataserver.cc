#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));
  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, 0, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, 0, true));
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  if(block_id >= KDefaultBlockCnt || block_id < 0){
    return {};
  }

  auto bm = this->block_allocator_->bm;
  auto blockSize = bm->block_size();
  std::vector<u8> buffer(blockSize);
  
  bm->read_block(0, buffer.data()); //get the version block
  if(!(buffer[block_id]&1))
    return {};
  auto thisVersion = buffer[block_id];
  if(thisVersion != version){
    return {};
  }

  auto read_res = bm->read_block(block_id, buffer.data());
  if(read_res.is_err()){
    return {};
  }

  std::vector<u8> res(len);
  memcpy(res.data(), buffer.data() + offset, len);  
  return res;
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  if(block_id >= KDefaultBlockCnt || block_id <= 0 || offset + buffer.size() > 4096){
    return false;
  }

  auto bm = this->block_allocator_->bm;
  auto blockSize = bm->block_size();
  std::vector<u8> buf(blockSize);

  bm->read_block(0, buf.data()); //get the version
  if(!(buf[block_id]&1))
    return false;

  auto write_res = bm->write_partial_block(block_id, buffer.data(), offset, buffer.size());
  if(write_res.is_ok()) 
    return true;

  return false;
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto bm = block_allocator_->bm;
  auto blockSize = bm->block_size();
  std::vector<u8> buf(blockSize);

  bm->read_block(0, buf.data());

  block_id_t res_id = 0;
  version_t res_v = 0;
  for(size_t i = 1; i < KDefaultBlockCnt; ++i){
    if(buf[i] & 1) 
      continue;
    buf[i]++;
    res_v = buf[i];
    res_id = i;
    break;
  }
  if(!res_id){
    return std::make_pair(res_id, res_v);
  }

  bm->write_block(0, buf.data());
  std::vector<u8> zero_buf(blockSize);
  memset(zero_buf.data(), 0, blockSize);
  bm->write_block(res_id, zero_buf.data()); //create an empty block

  return {std::make_pair(res_id, res_v)};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  if(block_id > KDefaultBlockCnt || block_id <= 0){
    return false;
  }

  auto bm = this->block_allocator_->bm;
  auto blockSize = bm->block_size();
  std::vector<u8> buf(blockSize);

  bm->read_block(0, buf.data());
  if(!(buf[block_id] & 1)){ //这个块没有被分配过
    return false;
  }
  buf[block_id]++;//将这个块的version+1，相当于创建了一个新的block，原本的block的内容也就被清除了
  bm->write_block(0, buf.data());
  return true;
}
} // namespace chfs