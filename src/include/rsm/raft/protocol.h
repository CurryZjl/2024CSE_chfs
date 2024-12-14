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

template <typename Command>
struct AppendEntriesArgs {
    /* Lab3: Your code here */
    int term; //leader’s term
    int leaderId; //so follower can redirect client
    int prevLogIndex; //index of log entry immediately preceding new ones
    int prevLogTerm; //term of prevLogIndex entry
   // std::vector<RaftLogEntry<Command>> entries;
    int leaderCommit; //leader’s commitIndex

     MSGPACK_DEFINE(
        term,
        leaderId,
        prevLogIndex,
        prevLogTerm,

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
    return RpcAppendEntriesArgs();
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    /* Lab3: Your code here */
    return AppendEntriesArgs<Command>();
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

    MSGPACK_DEFINE(
    
    )
};

struct InstallSnapshotReply {
    /* Lab3: Your code here */

    MSGPACK_DEFINE(
    
    )
};

} /* namespace chfs */