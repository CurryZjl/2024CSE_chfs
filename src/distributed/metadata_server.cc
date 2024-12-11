#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  // UNIMPLEMENTED();
try{
  inode_mtx[parent].lock();

  txn_id_t txn_id;
  if(is_log_enabled_){
    txn_id = commit_log->alloc_txn(ServerOperation(type, parent, name));
  }

  std::vector<std::pair<std::string, inode_id_t>> dir_content = readdir(parent);
  for(auto &entry : dir_content){
    if(entry.first == name){
      std::cerr << "replicated name!" << std::endl;
      inode_mtx[parent].unlock();
      return 0;
    }
  }
  fs_mtx.lock();
  auto alloc_inode_res = operation_->alloc_inode((InodeType)type);
  fs_mtx.unlock();
  if(alloc_inode_res.is_err()){
      std::cerr << "allocate inode error!" << std::endl;
      inode_mtx[parent].unlock();
      return 0;
  }
  inode_id_t new_inode_id = alloc_inode_res.unwrap();
  inode_mtx[new_inode_id].lock();

  if(is_log_enabled_){
    logging_mtx.lock();
    std::vector<std::shared_ptr<BlockOperation>> ops;
    ops.clear();
    std::vector<u8>tmp_buffer(DiskBlockSize);

    //block bitmap
    operation_->block_manager_->read_block(1 + operation_->inode_manager_->n_table_blocks + operation_->inode_manager_->n_bitmap_blocks, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(1 + operation_->inode_manager_->n_table_blocks + operation_->inode_manager_->n_bitmap_blocks, tmp_buffer));

    //inode bitmap
    operation_->block_manager_->read_block(1 + operation_->inode_manager_->n_table_blocks, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(1 + operation_->inode_manager_->n_table_blocks, tmp_buffer));

    //inode table
    auto inode_per_block = operation_->block_manager_->block_size() / sizeof(block_id_t);
    auto table_block_id = 1 + (new_inode_id-1) / inode_per_block;
    operation_->block_manager_->read_block(table_block_id, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(table_block_id, tmp_buffer));

    //inode at blockid
    block_id_t inode_block_id = operation_->inode_manager_->get(new_inode_id).unwrap();
    operation_->block_manager_->read_block(inode_block_id, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(inode_block_id, tmp_buffer));
    logging_mtx.unlock();
    commit_log->append_log(txn_id, ops);
  }

  dir_content.push_back(std::make_pair(name, new_inode_id));

  std::ostringstream oss;
  usize cnt = 0;
  size_t dirSize = dir_content.size();
  for (const auto &entry : dir_content) {
    oss << entry.first << ':' << entry.second;
    if (cnt < dirSize - 1) {
      oss << '/';
    }
    cnt += 1;
  }

  std::string str = oss.str();
  std::vector<u8> buffer;
  size_t strSize = str.size();
  for(size_t i = 0; i < strSize; i++){
    buffer.push_back(str[i]);
  }
  auto write_dir_res = operation_->write_file(parent, buffer);

  if(is_log_enabled_){
    logging_mtx.lock();
    std::vector<std::shared_ptr<BlockOperation>> ops;
    ops.clear();
    std::vector<u8>tmp_buffer(DiskBlockSize);
    block_id_t parent_block_id = operation_->inode_manager_->get(parent).unwrap();
    operation_->block_manager_->read_block(parent_block_id, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(parent_block_id, tmp_buffer));
    logging_mtx.unlock();
    commit_log->append_log(txn_id,ops);
  }

  inode_mtx[new_inode_id].unlock();
  inode_mtx[parent].unlock();
  if(write_dir_res.is_err()){
    std::cerr << "write back error!" << std::endl;
    return 0;
  }
  if(is_log_enabled_){
      commit_log->commit_log(txn_id);
  }

  return new_inode_id;
}
catch(std::exception &e){
    std::cout << e.what()  << std::endl;
    std::cout << "error in makenode"  << std::endl;
    return 0;
  }

}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  inode_mtx[parent].lock();
  lookup_mtx[parent].lock();
  txn_id_t txn_id;
  if(is_log_enabled_){
    txn_id = commit_log->alloc_txn(ServerOperation(0, parent, name));
  }

  std::vector<std::pair<std::string, inode_id_t>> dir_content = readdir(parent);
  inode_id_t inode_id = lookup(parent, name);
  lookup_mtx[parent].unlock();
  if(!inode_id)
  {
    inode_mtx[parent].unlock();
    return false;
  }
  inode_mtx[inode_id].lock();
  fs_mtx.lock();
  auto remove_res=operation_->remove_file(inode_id);
  fs_mtx.unlock();

  if(remove_res.is_err())
  {
    inode_mtx[parent].unlock();
    inode_mtx[inode_id].unlock();
    return false;
  }

  if(is_log_enabled_){
    logging_mtx.lock();
    std::vector<std::shared_ptr<BlockOperation>> ops;
    ops.clear();
    std::vector<u8> tmp_buffer(DiskBlockSize);
    auto bm = operation_->block_manager_;
    auto im = operation_->inode_manager_;

    //block bitmap
    bm->read_block(1 + im->n_table_blocks + im->n_bitmap_blocks, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(1 + im->n_table_blocks + im->n_bitmap_blocks, tmp_buffer));

    //inode bitmap
    bm->read_block(1 + im->n_table_blocks, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(1 + im->n_table_blocks, tmp_buffer));

    //inode table
    auto inode_per_block = bm->block_size() / sizeof(block_id_t);
    auto table_block_id = 1 + (inode_id-1) / inode_per_block;
    bm->read_block(table_block_id, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(table_block_id, tmp_buffer));

    //inode at blockid
    block_id_t inode_block_id = im->get(inode_id).unwrap();
    bm->read_block(inode_block_id, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(inode_block_id, tmp_buffer));
    logging_mtx.unlock();
    commit_log->append_log(txn_id, ops);
  }

  int content_pos = -1;
  size_t dir_content_size = dir_content.size();
  for(int i = 0; i < dir_content_size; i++)
    if (dir_content[i].second == inode_id) {
      content_pos=i;
      break;
    }
  dir_content.erase(dir_content.begin() + content_pos);

  std::ostringstream oss;
  usize cnt = 0;
  dir_content_size = dir_content.size();
  for (const auto &entry : dir_content) {
    oss << entry.first << ':' << entry.second;
    if (cnt < dir_content_size - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  std::string str = oss.str();
  std::vector<u8>vec;
  vec.clear();
  size_t str_size = str.size();
  for(int i = 0; i < str_size; i++)
    vec.push_back(str[i]);
  //std::cout << "test: " << vec.size() << std::endl;
  auto write_dir_res = operation_->write_file(parent, vec);

  if(is_log_enabled_){
    logging_mtx.lock();
    std::vector<std::shared_ptr<BlockOperation>> ops;
    ops.clear();
    std::vector<u8>tmp_buffer(DiskBlockSize);
    block_id_t parent_block_id = operation_->inode_manager_->get(parent).unwrap();
    operation_->block_manager_->read_block(parent_block_id, tmp_buffer.data());
    ops.push_back(std::make_shared<BlockOperation>(parent_block_id, tmp_buffer));
    logging_mtx.unlock();
    commit_log->append_log(txn_id,ops);
  }

  inode_mtx[parent].unlock();
  inode_mtx[inode_id].unlock();
  if (write_dir_res.is_err()) {
    return false;
  }

  if(is_log_enabled_)
  {
    commit_log->commit_log(txn_id);
  }
  return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto lookup_res = operation_->lookup(parent, name.c_str());
  if(lookup_res.is_err())
    return 0;
  return lookup_res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto block_size = operation_->block_manager_->block_size();

  std::vector<u8> inode(block_size);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inode_res = operation_->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    return {};
  }

  const auto content_sz = inode_p->get_size();
  auto block_cnt = content_sz / block_size;
  if(content_sz % block_size)
    block_cnt++;

  std::vector<BlockInfo>ret;
  ret.clear();

  for(int i = 0; i < block_cnt; i++)
  {
    block_id_t block_id = inode_p->blocks[i<<1];
    block_id_t mixed_id = inode_p->blocks[(i<<1)+1];
    mac_id_t mac_id = mixed_id >> (u64)32; //0-31 bit
    version_t version = mixed_id % (u64)32; //last 4bits
    ret.push_back(std::make_tuple(block_id, mac_id, version));
  }
  return ret;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto block_size = operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  inode_mtx[id].lock();
  auto inode_res = operation_->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    inode_mtx[id].unlock();
    return {};
  }

  const auto content_sz = inode_p->get_size();
  auto block_cnt = content_sz / block_size;
  if(content_sz % block_size)
    block_cnt++;
  //ask for block
  auto block_id = 0;
  auto mac_id = 0;
  auto version = 0;
  mac_id_t generated_id = generator.rand(1,num_data_servers);

  int try_offset = -1;
  while(1)
  {
    try_offset++;
    auto cur_id = (generated_id + try_offset) % num_data_servers; //current mac_id
    if(cur_id == 0)cur_id = num_data_servers;
    mac_mtx[cur_id].lock();
    auto alloc_res = clients_[cur_id]->call("alloc_block");
    mac_mtx[cur_id].unlock();
    if(alloc_res.is_err())continue;
    auto response =  alloc_res.unwrap();
    auto bv = response->as<std::pair<block_id_t, version_t>>();
    block_id = bv.first;
    if(!block_id)
      continue;
    version = bv.second;
    mac_id = cur_id;
    break;
  }
  if(!block_id || !mac_id)
  {
    inode_mtx[id].unlock();
    return {};
  }

  //modify inode
  inode_p->inner_attr.mtime = time(0);
  inode_p->blocks[block_cnt<<1] = block_id;
  block_id_t mixed_id = ((u64)mac_id << 32ull) + (u64)version;
  inode_p->blocks[(block_cnt<<1)+1] = mixed_id;
  inode_p->inner_attr.size += block_size;

  //write back
  inode_p->inner_attr.set_all_time(time(0));

  auto write_res =
      operation_->block_manager_->write_block(inode_res.unwrap(), inode.data());
  inode_mtx[id].unlock();
  if (write_res.is_err()) {
    return {};
  }
  return std::make_tuple(block_id, mac_id, version);
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  const auto block_size = operation_->block_manager_->block_size();

  std::vector<u8> inode(block_size);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  inode_mtx[id].lock();
  auto inode_res = operation_->inode_manager_->read_inode(id, inode);
  if (inode_res.is_err()) {
    inode_mtx[id].unlock();
    return {};
  }

  const auto content_sz = inode_p->get_size();
  auto block_cnt = content_sz / block_size;
  if(content_sz % block_size)block_cnt++;
  //find the block
  block_id_t block_pos = 0;
  for(int i = 0; i < block_cnt; i++)
  {
    block_id_t bid = inode_p->blocks[i<<1];
    block_id_t mixed_id = inode_p->blocks[(i<<1)+1];
    mac_id_t mid = mixed_id >> (u32)32;
    if(bid == block_id && mid == machine_id)
    {
      block_pos = i+1;
      break;
    }
  }
  if(!block_pos)
  {
    inode_mtx[id].unlock();
    return false;
  }
  block_pos--;
  //call free_block
  mac_mtx[machine_id].lock();
  auto free_res = clients_[machine_id]->call("free_block",block_id);
  mac_mtx[machine_id].unlock();
  if(free_res.is_err())
  {
    inode_mtx[id].unlock();
    return false;
  }
  auto response =  free_res.unwrap();
  auto resval = response->as<bool>();
  if(!resval)
  {
    inode_mtx[id].unlock();
    return false;
  }

  //modify the inode
  inode_p->inner_attr.mtime = time(0);
  inode_p->blocks[block_pos<<1] = inode_p->blocks[(block_pos<<1)+1] = 0;
  for(int i = block_pos+1; i < block_cnt; i++)
  {
    inode_p->blocks[(i<<1)-2] = inode_p->blocks[i<<1];
    inode_p->blocks[(i<<1)-1] = inode_p->blocks[(i<<1)+1];
  }
  inode_p->inner_attr.size -= block_size;
  //write back
  
  inode_p->inner_attr.set_all_time(time(0));
  auto write_res =
      operation_->block_manager_->write_block(inode_res.unwrap(), inode.data());
  inode_mtx[id].unlock();
  if (write_res.is_err()) {
    return false;
  }
  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  std::vector<std::pair<std::string, inode_id_t>> ret;
  ret.clear();
  auto read_dir_res = operation_->read_file(node);
  if (read_dir_res.is_err()) {
    return {};
  }

  auto dir_content = read_dir_res.unwrap();
  std::string str="";
  for(int i=0;i<dir_content.size();i++)
    str+=dir_content[i];
  // parse_directory(str, list);
  std::istringstream iss(str);
  std::string entry;

  while (std::getline(iss, entry, '/')) {
    size_t pos = entry.find(':');
    if (pos != std::string::npos) {
      std::string name = entry.substr(0, pos);
      std::string id_str = entry.substr(pos + 1);
      std::stringstream ss(id_str);
      inode_id_t id;
      ss >> id;
      ret.push_back(std::make_pair(name, id));
    }
  }
  
  return ret;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  // TODO: Implement this function.
  //UNIMPLEMENTED();
  auto getattr_res = operation_->get_type_attr(id);
  if(getattr_res.is_err())return {};
  std::pair<InodeType, FileAttr> res = getattr_res.unwrap();
  InodeType type = res.first;
  FileAttr fillAttr = res.second;
  return std::make_tuple(fillAttr.size, fillAttr.atime, fillAttr.mtime, fillAttr.ctime, (u8)type);
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs