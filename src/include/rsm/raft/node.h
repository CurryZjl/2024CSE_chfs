#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    /*utils*/
    void generateRandomTimeout();

    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    /* Lab3: Your code here */
    /*Persistent state on all servers*/
    std::vector<RaftLogEntry<Command>> log;
    int votedFor = -1; //candidateId that received vote in current term (or -1 if none)

    /*Volatile state on all servers*/
    int commitIndex; //index of highest log entry known to be committed (initialized to 0, increases monotonically)
    int lastApplied; //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

    int votesReceivedNum = 0;
    std::vector<bool> votesFrom;

    std::chrono::system_clock::time_point lastRPC;
    std::chrono::system_clock::duration electionTimeout; //这个时间记录收到leader心跳的间隔，如果超过，timeout，说明leader可能挂了
    std::chrono::system_clock::duration candidateTimeout; //这个时间记录当前自己作为candidate发起选举的持续时间，如果timeout说明这个term内没有成功选出leader

    /*Volatile state in leaders*/
    std::vector<int> nextIndex; //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    std::vector<int> matchIndex; //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    std::vector<int> matchCount;
    
    std::vector<u8> snapshot;

    /*解决并行问题 for Test #13: RaftTestPart3.UnreliableAgree*/
    std::condition_variable cv_election;

    /*defined for debug*/
    bool lockVerbose = false;

};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1)
{
    std::unique_lock<std::mutex> lock(mtx);

    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

   /* Lab3: Your code here */ 
   /*this lab provides a thread pool to handle asynchronous events.*/
    thread_pool = std::make_unique<ThreadPool>(32);

    std::string bm_path = "/tmp/raft_log/node" + std::to_string(my_id);
    auto bm = std::shared_ptr<BlockManager>(new BlockManager(bm_path, KDefaultBlockCnt));
    log_storage = std::make_unique<RaftLog<Command>>(bm);
    log_storage->node_id = my_id;
    state = std::make_unique<StateMachine>();

    if(log_storage->restore()){
        current_term = log_storage->current_term;
        votedFor = log_storage->votedFor;
        log = log_storage->log;
        snapshot = log_storage->snapshot;
    } else {
        log.clear();
        log.push_back(RaftLogEntry<Command>(0, 0, Command(0)));
        snapshot.clear();
        votedFor = -1;
        log_storage->current_term = current_term;
        log_storage->votedFor = votedFor;
        log_storage->log = log;
        log_storage->snapshot = snapshot;
        log_storage->saveAll();
    }
    
    if (!snapshot.empty()) {
        state->apply_snapshot(snapshot);
    }
    
    commitIndex = log.front().index;
    lastApplied = log.front().index;

    votesFrom.clear();
    //记录自己得到的选票
    votesFrom.assign(configs.size(), false);
    votesReceivedNum = 0;
    
    nextIndex.clear();
    matchIndex.clear();
    matchCount.clear();
    nextIndex.assign(configs.size(), 1); //初始状态下，对于其他每个server的nextIndex都是1
    matchIndex.assign(configs.size(), 0);

    //上次进行RPC操作的时间
    lastRPC = std::chrono::system_clock::now();
    generateRandomTimeout();

    lock.unlock();
    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    /* Lab3: Your code here */

    stopped.store(false);
    std::unique_lock<std::mutex> lock(mtx);
    /*raft算法是异步的，需要在后台监控各种事件（比如心跳检测）
    * 一个Node启动时需要同时启动对于后台事件的监听
    */
    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    

    size_t nodesSize = node_configs.size();
    for(int i = 0; i < nodesSize; i++){
        //注册所有clients
        auto cli = std::make_unique<RpcClient>(node_configs[i].ip_address, node_configs[i].port, true);
        rpc_clients_map.insert(std::make_pair(node_configs[i].node_id, std::move(cli)));
    }
    lock.unlock();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    stopped.store(true);
    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    size_t nodesSize = node_configs.size();
    for(int i = 0; i < nodesSize; i++)
        if(rpc_clients_map[i]){
            rpc_clients_map[i].reset();
        }
    lock.unlock();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    /* Lab3: Your code here */
    if(role == RaftRole::Leader)
        return std::make_tuple(true, current_term);
    return std::make_tuple(false, current_term);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    /* Lab3: Your code here */
    //only leader can apply new command
    if(role != RaftRole::Leader)
        return std::make_tuple(false, current_term, log.back().index);
    
    std::unique_lock<std::mutex> lock(mtx);
    int nextI = log.back().index + 1;
    Command cmd;
    cmd.deserialize(cmd_data, cmd_size);
    //添加新的entry
    log.push_back(RaftLogEntry<Command>(nextI, current_term, cmd));

    nextIndex[my_id] = nextI + 1; //我自己的nextIndex就是再加一
    matchIndex[my_id] = nextI; //我自己的需要被replicated的index
    matchCount.push_back(1);

    size_t size =  log.size();
    log_storage->updateLogsWithRange(log, size - 1, size);

    lock.unlock();
    return std::make_tuple(true, current_term, nextI);
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    /* Lab3: Your code here */ 
    std::unique_lock<std::mutex> lock(mtx);

    snapshot = state->snapshot();


    RaftLogEntry<Command> lastLog;
    if (lastApplied <= log.back().index) {
        lastLog = log[lastApplied - log.front().index];
        log.erase(log.begin(), log.begin() + lastApplied - log.front().index);
    }
    else {
        lastLog = log.back();
        log.clear();
    }
    if(log.empty())log.push_back(lastLog);

    log_storage->updateSnapshot(snapshot);
    log_storage->updateLogs(log);

    lock.unlock();

    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    /* Lab3: Your code here */
    return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    this->lastRPC = std::chrono::system_clock::now();
    /* Receiver implementation:
    * 1. Reply false if term < currentTerm (§5.1)
    */
    RequestVoteReply reply;
    //如果收到过时的请求，都应该拒绝该请求
    if(args.term < current_term){
        reply.term = current_term;
        reply.voteGranted = false;
    } else {
        if(args.term > current_term){
            //update
            /*  rule to all servers
            * if args.term > current_term, set current_term to T, convert to follower
            */
            current_term = args.term;
            role = RaftRole::Follower;
            leader_id = -1;
            votedFor = -1;
        } 
        //在当前term得到了一个投票请求
        /*  2. If votedFor is null or candidateId, and candidate’s log is at
        * least as up-to-date as receiver’s log, grant vote (§5.2, §5.4) 不是所有人都能成为leader，需要candidate的log最少比我自己的一样
        */
        reply.term = current_term;
        if((votedFor == -1 || votedFor == args.candidateId) && 
                (args.lastLogTerm > log.back().term || 
                (args.lastLogTerm == log.back().term && args.lastLogIndex >= log.back().index))){
            //第一次投票（或者已经投过票）
            reply.voteGranted = true;
            votedFor = args.candidateId;
            //唤醒所有等待线程,避免错误丢失投票信息
            cv_election.notify_all();
        }else{
            reply.voteGranted = false;
        }
    }

    log_storage->updateMeta(current_term, votedFor);
    lock.unlock();
    
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    if(reply.term > current_term){
        //update; there is a new leader
        current_term = reply.term;
        role = RaftRole::Follower;
        leader_id = -1;
        votedFor = -1;
        log_storage->updateMeta(current_term, votedFor);
    }
    if(role == RaftRole::Candidate){
        //在选举阶段
        if(reply.voteGranted && !votesFrom[target]){
            //刚刚得到了一个新的选票
            votesReceivedNum++;
            votesFrom[target] = true;
            if(votesReceivedNum > node_configs.size() / 2){
                //get majority
                role = RaftRole::Leader;
                leader_id = my_id;

                nextIndex.assign(node_configs.size(), log.back().index + 1);
                matchIndex.assign(node_configs.size(), 0);
                matchIndex[my_id] = log.back().index;
                matchCount.assign(log.back().index - commitIndex, 0);
            }
        }
    }
    lock.unlock();
    return;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    lastRPC = std::chrono::system_clock::now();
    chfs::AppendEntriesArgs<Command> args = transform_rpc_append_entries_args<Command>(rpc_arg);

    chfs::AppendEntriesReply reply;

    if (args.term < current_term) {
        reply.term = current_term;
        reply.success = false;
        return reply;
    }

    if(args.term > current_term || role == RaftRole::Candidate){
        current_term = args.term;
        role = RaftRole::Follower;
        leader_id = -1;
        votedFor = -1;
        log_storage->updateMeta(current_term, votedFor);
    }

    reply.success = false;
   
    if (args.prevLogIndex <= log.back().index && args.prevLogTerm == log[args.prevLogIndex - log.front().index].term) {
        if (!args.entries.empty()) {
            if (args.prevLogIndex < log.back().index) {

                log.erase(log.begin() + args.prevLogIndex + 1 - log.front().index, log.end());
                int tmpLogSize = log.size();
                log.insert(log.end(), args.entries.begin(), args.entries.end());
                log_storage->updateLogsWithRange(log, tmpLogSize, log.size());
            }
            else {
                int tmpLogSize = log.size();
                log.insert(log.end(), args.entries.begin(), args.entries.end());
                log_storage->updateLogsWithRange(log, tmpLogSize, log.size());
            }
        }

        if (args.leaderCommit > commitIndex) {
            commitIndex = std::min(args.leaderCommit, log.back().index);
        }

        reply.success = true;
    }

    reply.term = current_term;
    lock.unlock();
    return reply;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);

    if(reply.term > current_term){
        current_term = arg.term;
        role = RaftRole::Follower;
        leader_id = -1;
        votedFor = -1;
        log_storage->updateMeta(current_term, votedFor);
        return;
    }

    if (role == RaftRole::Leader) {
        if (reply.success) {
            int tmpIndex = matchIndex[node_id];
            matchIndex[node_id] = std::max(matchIndex[node_id], (int)(arg.prevLogIndex + arg.entries.size()));
            nextIndex[node_id] = matchIndex[node_id]+1;

            tmpIndex = std::max(tmpIndex - commitIndex, 0) - 1;
            for (int i = matchIndex[node_id] - commitIndex - 1; i > tmpIndex; i--) {
                matchCount[i]++;
                if (matchCount[i] > node_configs.size() / 2 ) {
                    commitIndex += i + 1;
                    matchCount.erase(matchCount.begin(), matchCount.begin() + i + 1);
                    break;
                }
            }
        } else {
            nextIndex[node_id] = std::min(nextIndex[node_id], arg.prevLogIndex);
        }
    }
    lock.unlock();
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    /* Lab3: Your code here */
    std::unique_lock<std::mutex> lock(mtx);
    lastRPC = std::chrono::system_clock::now();
    InstallSnapshotReply reply;
    reply.term = current_term;

   /*Reply immediately if term < currentTerm*/
    if (args.term < current_term) {
        return reply;
    }

    if (args.term > current_term || role == RaftRole::Candidate) {
        current_term = args.term;
        role = RaftRole::Follower;
        leader_id = -1;
        votedFor = -1;
        log_storage->updateMeta(current_term, votedFor);
    }

    if (args.lastIncludedIndex <= log.back().index && args.lastIncludedTerm == log[args.lastIncludedIndex - log.front().index].term) {
        if (args.lastIncludedIndex <= log.back().index) {
            
            log.erase(log.begin(), log.begin() + args.lastIncludedIndex - log.front().index);
        }
        else {
            /*Discard the entire log*/
            log.clear();
        }
    }
    else {
        //已经进行过snapshot，新添加一个logEntry
        log.assign(1, RaftLogEntry<Command>(args.lastIncludedIndex, args.lastIncludedTerm));
    }
    /*Reset state machine using snapshot contents (and load snapshot’s cluster configuration)*/
    snapshot = args.snapshot;
    state->apply_snapshot(snapshot);

    lastApplied = args.lastIncludedIndex;
    commitIndex = std::max(commitIndex, args.lastIncludedIndex);

    log_storage->updateLogs(log);
    log_storage->updateSnapshot(args.snapshot);

    lock.unlock();
    return reply;
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    /* Lab3: Your code here */
     std::unique_lock<std::mutex> lock(mtx);

    if (reply.term > current_term) {
        current_term = reply.term;
        role = RaftRole::Follower;
        leader_id = -1;
        votedFor = -1;
        log_storage->updateMeta(current_term, votedFor);
        return;
    }
    if (role != RaftRole::Leader) {
        return;
    }

    matchIndex[node_id] = std::max(matchIndex[node_id], arg.lastIncludedIndex);
    nextIndex[node_id] = matchIndex[node_id] + 1;

    lock.unlock();
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            if(!rpc_clients_map[target_id]){
                auto cli = std::make_unique<RpcClient>(node_configs[target_id].ip_address, node_configs[target_id].port, true);
                rpc_clients_map.insert(std::make_pair(node_configs[target_id].node_id, std::move(cli)));
            }
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
            if(!rpc_clients_map[target_id]){
                auto cli = std::make_unique<RpcClient>(node_configs[target_id].ip_address, node_configs[target_id].port, true);
                rpc_clients_map.insert(std::make_pair(node_configs[target_id].node_id, std::move(cli)));
            }
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Utils

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::generateRandomTimeout() {
    static std::random_device rd;
    static std::minstd_rand gen(rd());
    static std::uniform_int_distribution<int> electionDis(300, 500);
    static std::uniform_int_distribution<int> candidateDis(800, 1000);
    electionTimeout = std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(electionDis(gen)));
    candidateTimeout = std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(candidateDis(gen)));
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    /* Uncomment following code when you finish */
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    std::chrono::system_clock::time_point current_time;

    while (true) {
        {
            //自己本来就挂了
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            lock.lock();
            current_time = std::chrono::system_clock::now();
            //election timeout, 心跳断了，怀疑原本的leader挂了，现在需要发送请求让自己成为新的leader
            //or原本就已经处在选举阶段，但是在candidateTimeout时间段内没有完成选举
            //就需要关闭这个term重新发起新的term进行选举
            if((role == RaftRole::Follower && current_time - lastRPC > electionTimeout)
                || (role == RaftRole::Candidate && current_time - lastRPC > candidateTimeout))
            {
                if(is_stopped()){
                    return;
                }
                role = RaftRole::Candidate;
                //start new election
                current_term++;
                votedFor = my_id;
                log_storage->updateMeta(current_term, votedFor);
                votesReceivedNum = 1;
                votesFrom.assign(node_configs.size(),false);
                votesFrom[my_id] = true;

                //生成随机的timeout
                generateRandomTimeout();
                leader_id = -1;

                RequestVoteArgs request_args;
                request_args.term = current_term;
                request_args.candidateId = my_id;
                request_args.lastLogIndex = log.back().index;
                request_args.lastLogTerm = log.back().term;
                size_t size =  node_configs.size();
                for (int target_id = 0; target_id < size; ++target_id) {
                    if (target_id != my_id) {
                        //使用thread_pool发送异步的选举请求
                        auto result = thread_pool->enqueue(&RaftNode::send_request_vote, this, target_id, request_args); // (function pointer, this pointer, argumen
                    }
                }
                lastRPC = std::chrono::system_clock::now(); 
            }

        }
        lock.unlock();
        //The background threads should sleep some time after each loop iteration, instead of busy-waiting the event.
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            lock.lock();
            if(role == RaftRole::Leader){
                int logBackIndex = log.back().index;
                size_t size = node_configs.size();
                for(int i = 0; i < size; i++){
                    if(i != my_id && nextIndex[i] <= logBackIndex){
                        if(nextIndex[i] > log.front().index){
                            //两台机器之间有共有的log entrys
                            AppendEntriesArgs<Command> args;
                            args.term = current_term;
                            args.leaderId = my_id;
                            args.leaderCommit = commitIndex;
                            args.prevLogIndex = nextIndex[i] - 1;
                            args.prevLogTerm = log[args.prevLogIndex - log.front().index].term;
                            args.entries.clear();
                            auto offset = log.begin() - log.front().index;
                            if(nextIndex[i] < logBackIndex + 1){
                                args.entries.assign(offset + nextIndex[i],
                                        offset + logBackIndex + 1);
                            }
                            thread_pool->enqueue(&RaftNode::send_append_entries, this, i, args);
                        } else {
                            InstallSnapshotArgs args;
                            args.term = current_term;
                            args.leaderId = my_id;
                            args.lastIncludedIndex = log.front().index;
                            args.lastIncludedTerm = log.front().term;
                            args.snapshot = snapshot;
                            thread_pool->enqueue(&RaftNode::send_install_snapshot, this, i, args);
                        }
                    }
                }
            }

        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /* Uncomment following code when you finish */
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);
    std::vector<RaftLogEntry<Command>> LogsToApply;

    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            lock.lock();
            //若最近commit的cmd还没有被apply
            if(commitIndex > lastApplied) {
                LogsToApply.clear();
                auto offset = log.begin() + 1 - log.front().index; //log's iterater 需要apply从lastApplied到commitIndex之间的日志内容
                LogsToApply.assign(offset + lastApplied, offset + commitIndex);
                for (auto & entry : LogsToApply) {
                    state->apply_log(entry.cmd);
                }   
                lastApplied = commitIndex;
            }
        }
        lock.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(40));
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    /* Uncomment following code when you finish */
    std::unique_lock<std::mutex> lock(mtx, std::defer_lock);

    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            /* Lab3: Your code here */
            lock.lock();
            //now it's Leader
            if (role == RaftRole::Leader) {
                AppendEntriesArgs<Command> arg;
                arg.term = current_term;
                arg.leaderId = my_id;
                arg.entries = {}; //empty
                arg.leaderCommit = commitIndex;
                size_t size = node_configs.size();
                for (int target_id = 0; target_id < size; ++target_id) {
                    if (target_id != my_id) {

                        arg.prevLogIndex = nextIndex[target_id] - 1;
                        if(arg.prevLogIndex < log.front().index){
                            arg.prevLogIndex = log.front().index;
                        }
                        arg.prevLogTerm = log[arg.prevLogIndex  - log.front().index].term;
                        //向其他clients发送心跳
                        auto result = thread_pool->enqueue(&RaftNode::send_append_entries, this, target_id, arg); // (function pointer, this pointer, argumen
                    }
                }
            }
            lock.unlock();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100)); 
    }

    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}