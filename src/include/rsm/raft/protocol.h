#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    /* Lab3: Your code here */
    int candidateId; //candidate requesting vote
    int term; //candidate's term
    int lastLogIndex; //index of candidate's last log entry
    int lastLogTerm; //term of candidate's last log entry
    
    MSGPACK_DEFINE(
        candidateId,
        term,
        lastLogIndex,
        lastLogTerm
    )
};

struct RequestVoteReply {
    /* Lab3: Your code here */
    int term; //currentTerm, for candidate's last log entry
    bool voteGranted; //true means candidate received vote
    
    MSGPACK_DEFINE(
        term,
        voteGranted
    )
};

//自定义logEntry的结构
template<typename Command>
class RaftLogEntry {
public:
    int index;
    int term;
    Command cmd;

    RaftLogEntry(int index = 0, int term = 0) : index(index), term(term) {}
    RaftLogEntry(int index, int term, Command cmd) : index(index), term(term), cmd(cmd) {}
};

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term; //leader’s term
    int leaderId; //so follower can redirect client
    int prevLogIndex; //index of log entry immediately preceding new ones
    int prevLogTerm; //term of prevLogIndex entry
    std::vector<RaftLogEntry<Command>> entries; //log entries to store (empty for heartbeat; may send more than one for efficiency)
    int leaderCommit; //leader’s commitIndex

     MSGPACK_DEFINE(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        entries,
        leaderCommit
    )
};

struct RpcAppendEntriesArgs {
    /* Lab3: Your code here */
    int term; //leader’s term
    int leaderId; //so follower can redirect client
    int prevLogIndex; //index of log entry immediately preceding new ones
    int prevLogTerm; //term of prevLogIndex entry
    std::vector<std::vector<u8>> cmds;
    std::vector<int> indexs;
    std::vector<int> terms;
    int leaderCommit; //leader’s commitIndex

    MSGPACK_DEFINE(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,
        cmds,
        indexs,
        terms,
        leaderCommit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    /* Lab3: Your code here */
    RpcAppendEntriesArgs rpc_arg;
    rpc_arg.term = arg.term;
    rpc_arg.leaderId = arg.leaderId;
    rpc_arg.prevLogIndex = arg.prevLogIndex;
    rpc_arg.prevLogTerm = arg.prevLogTerm;
    rpc_arg.leaderCommit = arg.leaderCommit;
    size_t size = arg.entries.size();
    for(int i = 0; i < size; i++){
        rpc_arg.indexs.push_back(arg.entries[i].index);
        rpc_arg.terms.push_back(arg.entries[i].term);
        rpc_arg.cmds.push_back(arg.entries[i].cmd.serialize(arg.entries[i].cmd.size()));
    }

    return rpc_arg;
    //return RpcAppendEntriesArgs();
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    //return AppendEntriesArgs<Command>();
     AppendEntriesArgs<Command> arg;
    arg.term = rpc_arg.term;
    arg.leaderId = rpc_arg.leaderId;
    arg.prevLogIndex = rpc_arg.prevLogIndex;
    arg.prevLogTerm = rpc_arg.prevLogTerm;
    arg.leaderCommit = rpc_arg.leaderCommit;
    arg.entries.clear();
    size_t size = rpc_arg.cmds.size();
    for(int i = 0; i < size; i++){
        arg.entries.push_back(RaftLogEntry<Command>(rpc_arg.indexs[i], rpc_arg.terms[i], Command()));
        arg.entries[i].cmd.deserialize(rpc_arg.cmds[i],rpc_arg.cmds[i].size());
    }

    return arg;
}

struct AppendEntriesReply {
    /* Lab3: Your code here */
    int term; //currentTerm, for leader to update itself
    bool success; //true if follower contained entry matching prevLogIndex and prevLogTerm


    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    /* Lab3: Your code here */
    int term; //leader's term
    int leaderId;
    int lastIncludedIndex; //the snapshot replaces all entries up through and including this index
    int lastIncludedTerm; //term of lastIncludedIndex
    std::vector<uint_least8_t> snapshot;

    MSGPACK_DEFINE(
        term,
        leaderId,
        lastIncludedIndex,
        lastIncludedTerm,
        snapshot
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */
    int term; //currentTerm, for leader to update itself

    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */