#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <sys/time.h>
#include <algorithm>
#include <thread>
#include <stdarg.h>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template <typename state_machine, typename command>
class raft
{
    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

    // #define RAFT_LOG(fmt, args...) \
//     do                         \
//     {                          \
//     } while (0);

#define RAFT_LOG(fmt, args...)                                                                                   \
    do                                                                                                           \
    {                                                                                                            \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role
    {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    /* for leader election*/
    int votedFor;
    int lastLogTerm;
    int lastLogIndex;
    int voteCount;

    /* for ping*/
    time_t lastRPCTime;
    time_t electionTimeout;

    /* for commit */
    int commitIndex;
    std::vector<log_entry<command>> logs;
    std::vector<int> prevLogIndex = std::vector<int>(num_nodes(), 0);
    int prevLogTerm;
    int appliedIndex;

    /* ----Persistent state on all server----  */

    /* ---- Volatile state on all server----  */

    /* ---- Volatile state on leader----  */

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes()
    {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    time_t getElectionTimeout();
    time_t getTime();
    void newTerm(int term, int leaderId);
    void newElection();
    int getCommitIndex();
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) : stopped(false),
                                                                                                                                               rpc_server(server),
                                                                                                                                               rpc_clients(clients),
                                                                                                                                               my_id(idx),
                                                                                                                                               storage(storage),
                                                                                                                                               state(state),
                                                                                                                                               background_election(nullptr),
                                                                                                                                               background_ping(nullptr),
                                                                                                                                               background_commit(nullptr),
                                                                                                                                               background_apply(nullptr),
                                                                                                                                               current_term(0),
                                                                                                                                               role(follower)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    votedFor = -1;
    lastLogIndex = 0;
    lastLogTerm = 0;
    voteCount = 0;
    prevLogTerm = 0;
    int clientNum = rpc_clients.size();
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft()
{
    if (background_ping)
    {
        delete background_ping;
    }
    if (background_election)
    {
        delete background_election;
    }
    if (background_commit)
    {
        delete background_commit;
    }
    if (background_apply)
    {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop()
{
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped()
{
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term)
{
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start()
{
    // Lab3: Your code here
    electionTimeout = getElectionTimeout();
    log_entry<command> empty_entry;
    empty_entry.index = 0;
    empty_entry.term = 0;
    logs.push_back(empty_entry);
    commitIndex = appliedIndex = 0;

    RAFT_LOG("start");
    lastRPCTime = getTime();
    srand(time(NULL));
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index)
{
    // Lab3: Your code here
    mtx.lock();
    if (role == leader)
    {
        RAFT_LOG("add a new command to index: %d", lastLogIndex + 1);
        log_entry<command> newEntry;
        newEntry.cmd = cmd;
        newEntry.term = current_term;
        newEntry.index = ++lastLogIndex;
        lastLogTerm = current_term;
        logs.push_back(newEntry);
        term = current_term;
        index = newEntry.index;
        mtx.unlock();
        return true;
    }
    else
    {
        term = current_term;
        mtx.unlock();
        return false;
    }
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot()
{
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
    reply.trueTerm = current_term;
    reply.leader_id = leader_id;
    if (args.term < current_term)
    {
        RAFT_LOG("reject %d's vote request", args.candidateId);
        reply.voteGranted = false;
        mtx.unlock();
        return 0;
    }

    if (args.term > current_term)
    {
        RAFT_LOG("become follower of new term and vote now", args.candidateId);
        newTerm(args.term, leader_id);
    }
    if ((votedFor == -1 || votedFor == args.candidateId) && (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)))
    {
        // after voting, update the election timeout
        RAFT_LOG("grant vote to %d", args.candidateId);
        lastRPCTime = getTime();
        votedFor = args.candidateId;
        reply.voteGranted = true;
    }
    else
    {
        RAFT_LOG("reject %d's vote request", args.candidateId);
        reply.voteGranted = false;
    }
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
    if (role == candidate)
    {
        if (reply.trueTerm > current_term)
        {
            newTerm(reply.trueTerm, reply.leader_id);
            RAFT_LOG("handle_request_vote_reply: there's already a leader, become follower now");
        }
        else
        {
            if (reply.voteGranted)
            {
                voteCount++;
                RAFT_LOG("recieve vote from %d, voteCount: %d", target, voteCount);
                if (voteCount > rpc_clients.size() / 2)
                {
                    role = leader;
                    leader_id = my_id;
                    RAFT_LOG("voted to be leader now");
                }
            }
        }
    }
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
    // laeder's ping
    if (arg.term > current_term)
    {
        lastRPCTime = getTime();
        RAFT_LOG("get heart beat from a new leader %d", arg.leaderId);
        newTerm(arg.term, arg.leaderId);
        reply.term = current_term;
        reply.leader_id = leader_id;
    }
    else if (arg.term == current_term)
    {
        if (role == follower)
        {
            lastRPCTime = getTime();
            RAFT_LOG("get normal ping from leader");
        }
        if (role == candidate)
        {
            lastRPCTime = getTime();
            RAFT_LOG("fail in election, become follower");
            role = follower;
            leader_id = arg.leaderId;
        }
        reply.term = current_term;
        reply.leader_id = leader_id;
    }
    else
    {
        // just ignore
        reply.success = false;
        reply.term = current_term;
        reply.leader_id = leader_id;
        mtx.unlock();
        return 0;
    }

    // leader's append cmd
    if (!arg.empty)
    {
        int size = logs.size();
        int newLogSize = arg.entries.size();
        int prevIndex = arg.prevLogIndex;
        RAFT_LOG("append prevIndex: %d, fellow's size: %d", prevIndex, size);
        if (size <= prevIndex)
        {
            RAFT_LOG("follow's size is smaller than leader's prev");
            reply.success = false;
        }
        else if (logs[prevIndex].term != arg.prevLogTerm)
        {
            RAFT_LOG("mismatch bewtween follow's and leader's entry");
            for (int i = 0; i < size - prevIndex; i++)
            {
                logs.pop_back();
            }
            reply.success = false;
        }
        else
        {
            // append new logs
            RAFT_LOG("append entry to follower, size: %d", newLogSize - prevIndex - 1);
            reply.success = true;
            if (arg.leaderId != my_id)
            {
                int i;
                for (i = prevIndex + 1; i < newLogSize; i++)
                {
                    logs.push_back(arg.entries[i]);
                }
                lastLogIndex = logs.back().index;
                lastLogTerm = logs.back().term;
                // have to correct old stubborn leader
                if (arg.leaderCommit > commitIndex)
                {
                    commitIndex = std::min(arg.leaderCommit, i - 1);
                }
            }
        }
    }
    mtx.unlock();
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg, const append_entries_reply &reply)
{
    // Lab3: Your code here
    mtx.lock();
    if (reply.term > current_term)
    {
        RAFT_LOG("find new leader, become follower");
        newTerm(reply.term, reply.leader_id);
    }
    // for append log
    if (arg.empty == false && reply.success == false)
    {
        RAFT_LOG("follows' entry out of date, change prevIndex to: %d", arg.prevLogIndex - 1);
        if (arg.prevLogIndex >= 1)
            prevLogIndex[node] = arg.prevLogIndex - 1;
    }
    if (reply.success == true)
    {
        prevLogIndex[node] = arg.entries.size() - 1;
    }
    mtx.unlock();
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply)
{
    // Lab3: Your code here
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg, const install_snapshot_reply &reply)
{
    // Lab3: Your code here
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg)
{
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0)
    {
        handle_request_vote_reply(target, arg, reply);
    }
    else
    {
        RAFT_LOG("-------RPC FAILS-------");
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg)
{
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0)
    {
        handle_append_entries_reply(target, arg, reply);
    }
    else
    {
        RAFT_LOG("-------RPC FAILS-------");
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg)
{
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0)
    {
        handle_install_snapshot_reply(target, arg, reply);
    }
    else
    {
        RAFT_LOG("-------RPC FAILS-------");
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election()
{
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    RAFT_LOG("run_bg_election");
    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here
        mtx.lock();
        if (role == follower || role == candidate)
        {
            time_t current_time = getTime();
            // begin a election now
            time_t timeAfter = current_time - lastRPCTime;
            if (timeAfter > getElectionTimeout())
            {
                RAFT_LOG("electTimeout! begin an election");
                newElection();
                request_vote_args args;
                args.term = current_term;
                args.candidateId = my_id;
                args.LastLogIndex = lastLogIndex;
                args.LastLogTerm = lastLogTerm;
                for (int i = 0; i < rpc_clients.size(); i++)
                    thread_pool->addObjJob(this, &raft::send_request_vote, i, args);
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit()
{
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here
        mtx.lock();
        if (role == leader)
        {
            int tempCMIndex = getCommitIndex();
            if (tempCMIndex != logs.size() - 1)
            {
                append_entries_args<command> args;
                args.empty = false;
                args.entries = logs;
                args.leaderId = my_id;
                args.leaderCommit = commitIndex;
                args.term = current_term;
                args.prevLogTerm = prevLogTerm;
                for (int i = 0; i < rpc_clients.size(); i++)
                {
                    args.prevLogIndex = prevLogIndex[i];
                    thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
                }
            }
            else
            {
                RAFT_LOG("leader: finish raplica now");
                commitIndex = tempCMIndex;
            }
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply()
{
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here:
        mtx.lock();
        if(commitIndex>appliedIndex){
            state->apply_log(logs[++appliedIndex].cmd);
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping()
{
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true)
    {
        if (is_stopped())
            return;
        // Lab3: Your code here:
        mtx.lock();
        if (role == leader)
        {
            RAFT_LOG("%d send ping now", my_id);
            append_entries_args<command> args;
            args.empty = true;
            args.term = current_term;
            args.leaderId = my_id;
            for (int i = 0; i < rpc_clients.size(); i++)
                thread_pool->addObjJob(this, &raft::send_append_entries, i, args);
        }
        mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    return;
}

/******************************************************************

                        Other functions

*******************************************************************/
template <typename state_machine, typename command>
time_t raft<state_machine, command>::getElectionTimeout()
{
    // 150-300ms
    time_t random = 150 + rand() % 150;
    return random;
}

template <typename state_machine, typename command>
time_t raft<state_machine, command>::getTime()
{
    struct timeval time;
    gettimeofday(&time, NULL);
    return time.tv_sec * 1000 + time.tv_usec / 1000;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::newTerm(int term, int leaderId)
{
    role = follower;
    current_term = term;
    leader_id = leaderId;
    votedFor = -1;
    voteCount = 0;
    electionTimeout = getElectionTimeout();
}

template <typename state_machine, typename command>
void raft<state_machine, command>::newElection()
{
    role = candidate;
    current_term++;
    voteCount = 0;
    votedFor = my_id;
    electionTimeout = getElectionTimeout();
}

template <typename state_machine, typename command>
int raft<state_machine, command>::getCommitIndex()
{
    std::vector<int> copy = prevLogIndex;
    std::sort(copy.begin(), copy.end());
    return copy[copy.size() / 2];
}
#endif // raft_h