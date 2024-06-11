#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <vector>
#include <iostream>

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

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    int voted_for = -1; 
    std::vector<log_entry<command>> logs;
    int voted_me = 0;
    unsigned long timeout = 0;
    unsigned long last_res_time;
    int commit_idx = 0; 
    int last_applied = 0; 
    std::vector<int> next_index; 
    std::vector<int> match_index; 

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

    unsigned long get_current_time();

private:
    bool is_stopped();
    int num_nodes() { return rpc_clients.size(); }

    // background workers
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here
    void start_election();
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) : storage(storage),
                                                                                                                                               state(state),
                                                                                                                                               rpc_server(server),
                                                                                                                                               rpc_clients(clients),
                                                                                                                                               my_id(idx),
                                                                                                                                               stopped(false),
                                                                                                                                               role(follower),
                                                                                                                                               current_term(0),
                                                                                                                                               background_election(nullptr),
                                                                                                                                               background_ping(nullptr),
                                                                                                                                               background_commit(nullptr),
                                                                                                                                               background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    voted_for = -1;
    commit_idx = 0;
    last_applied = 0;
    voted_me = 0;
    last_res_time = get_current_time();
    srand((unsigned)time(NULL));
    std::string dir = std::string("raft_temp/raft_storage_") + std::to_string(my_id);
    storage = new raft_storage<command>(dir);
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
    logs.clear();
    log_entry<command> l;
    l.term = 0;
    l.index = 0;
    logs.push_back(l);
    std::vector<log_entry<command>> vec;
    storage->find_meta(current_term, voted_for);
    storage->append_meta(current_term, voted_for);
    storage->find_log(vec);
    if (vec.size() != 0)
    {
        logs.insert(logs.end(), vec.begin(), vec.end());
    }
    int total = num_nodes();
    int next = logs[logs.size() - 1].index + 1;
    for (int i = 0; i < total; i++)
    {
        next_index.push_back(next);
        match_index.push_back(0);
    }
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index)
{
    mtx.lock();
    if (role != raft_role::leader)
    {
        mtx.unlock();
        return false;
    }
    index = logs.size();
    term = current_term;
    log_entry<command> log;
    log.commandLog = cmd;
    log.index = index;
    log.term = current_term;
    logs.push_back(log);
    match_index[my_id] = index;
    std::vector<log_entry<command>> vec;
    vec.insert(vec.end(), logs.begin() + 1, logs.end());
    storage->append_log(vec);
    mtx.unlock();

    return true;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot()
{
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply)
{
    mtx.lock();
    reply.voteGranted = false;
    reply.term = args.term;
    bool isChangedTerm = false;
    bool isChangedVote = false;

    if (args.term == current_term)
    {
        int logSize = logs.size();
        if (voted_for == -1 || voted_for == args.candidateId)
        {
            if (logSize == 1 || args.lastLogTerm > logs[logSize - 1].term || (args.lastLogTerm == logs[logSize - 1].term && args.lastLogIndex >= logs[logSize - 1].index))
            {
                voted_for = args.candidateId;
                reply.voteGranted = true;
                isChangedVote = true;
            }
        }
    }
    else
    {
        if (args.term > current_term)
        {
            current_term = args.term;
            isChangedTerm = true;
            role = raft_role::follower;
            timeout = 300 + rand() % 201;
            int logSize = logs.size();
            if (logSize == 1 || args.lastLogTerm > logs[logSize - 1].term || (args.lastLogTerm == logs[logSize - 1].term && args.lastLogIndex >= logs[logSize - 1].index))
            {
                voted_for = args.candidateId;
                role = raft_role::follower;
                timeout = 300 + rand() % 201;
                reply.voteGranted = true;
                isChangedVote = true;

                if (role == raft_role::follower)
                {
                    last_res_time = get_current_time();
                }
            }
        }
    }
    if (isChangedTerm || isChangedVote)
    {
        storage->append_meta(current_term, voted_for);
        int t = 0, v = 0;
        storage->find_meta(t, v);
    }

    mtx.unlock();

    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply)
{
    mtx.lock();
    bool isChangedTerm = false;
    bool isChangedVote = false;
    int total = num_nodes();
    if (reply.term > current_term)
    {
        current_term = reply.term;
        role = follower;
        timeout = 300 + rand() % 201;
        isChangedTerm = true;
    }
    if (role == candidate)
    {
        if (reply.voteGranted == true)
        {
            voted_me++;
            if (voted_me >= (total + 1) / 2)
            {
                role = leader;
                voted_me = 0;
                voted_for = -1;
                isChangedVote = true;

                int next = logs[logs.size() - 1].index + 1;
                for (int i = 0; i < total; i++)
                {
                    next_index[i] = next;
                    match_index[i] = 0;
                }
                match_index[my_id] = logs[logs.size() - 1].index;
            }
        }
    }
    if (isChangedVote || isChangedTerm)
    {
        storage->append_meta(current_term, voted_for);
    }
    mtx.unlock();

    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply)
{
    mtx.lock();
    reply.success = true;
    reply.term = current_term;
    bool isChangedTerm = false;
    bool isChangedVote = false;

    if (arg.term < current_term)
    {
        reply.success = false;
    }
    else
    {
        if (arg.term > current_term)
        {
            if (role == raft_role::leader)
            {
            }
            role = raft_role::follower;
            timeout = 300 + rand() % 201;
            current_term = arg.term;
            isChangedTerm = true;
            storage->append_meta(current_term, voted_for);
        }

        int size = logs.size();
        if (arg.leaderCommit > commit_idx)
        {
            if (logs[size - 1].term == current_term)
            {
                commit_idx = std::min(arg.leaderCommit, size - 1);
            }
        }
        if (arg.isHeartBeat)
        {
            if (role == raft_role::candidate)
            {
                role = raft_role::follower;
                timeout = 300 + rand() % 201;
            }
            if (role == raft_role::leader)
            {
                role = raft_role::follower;
                timeout = 300 + rand() % 201;
            }
            if (voted_for != -1)
            {
                voted_for = -1;
                isChangedVote = true;
            }
        }
        else
        {
            if (size - 1 < arg.prevLogIndex || (arg.prevLogIndex > 0 && logs[arg.prevLogIndex].term != arg.prevLogTerm))
            {
                reply.success = false;
            }
            else
            {
                if (arg.prevLogIndex + 1 <= 0)
                {
                    logs.erase(logs.begin(), logs.end());
                    std::vector<class log_entry<command>>(logs).swap(logs);
                }
                else
                {
                    if (arg.prevLogIndex + 1 < (int)(logs.size()))
                    {
                        logs.erase(logs.begin() + arg.prevLogIndex + 1, logs.end());
                        std::vector<class log_entry<command>>(logs).swap(logs);
                    }
                }
                for (long unsigned int i = 0; i < arg.entries.size(); i++)
                {
                    logs.push_back(arg.entries[i]);
                }
                size = logs.size();
                std::vector<log_entry<command>> vec;
                vec.insert(vec.end(), logs.begin() + 1, logs.end());
                storage->append_log(vec);
            }
        }
    }
    last_res_time = get_current_time();
    if (isChangedVote || isChangedTerm)
    {
        storage->append_meta(current_term, voted_for);
    }
    mtx.unlock();

    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply)
{
    mtx.lock();
    if (role != raft_role::leader)
    {
        mtx.unlock();
        return;
    }
    if (reply.term > current_term)
    {
        if (role == raft_role::leader)
        {
        }
        current_term = reply.term;
        role = raft_role::follower;
        timeout = 300 + rand() % 201;
        storage->append_meta(current_term, voted_for);
    }
    else
    {
        if (!arg.isHeartBeat)
        {
            if (!reply.success)
            {
                append_entries_args<command> newArg;
                int tmp = next_index[target];
                next_index[target] = tmp - 1;
            }
            else
            {
                int last = arg.entries[arg.entries.size() - 1].index;
                next_index[target] = last + 1;
                match_index[target] = last;
            }
        }
    }

    mtx.unlock();

    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply)
{
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply)
{
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
        // RPC fails
    }
}

template <typename state_machine, typename command>
unsigned long raft<state_machine, command>::get_current_time()
{
    unsigned long now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    return now;
}

/******************************************************************

                        Background Workers

*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::start_election()
{
    current_term++;
    voted_for = my_id;
    voted_me = 1;
    storage->append_meta(current_term, voted_for);
    role = raft_role::candidate;
    request_vote_args args;
    args.candidateId = my_id;
    args.term = current_term;
    int logSize = logs.size();
    if (logSize == 1)
    {
        args.lastLogIndex = 0;
        args.lastLogTerm = 0;
    }
    else
    {
        args.lastLogIndex = logSize - 1;
        args.lastLogTerm = logs[logSize - 1].term;
    }
    int total = num_nodes();
    for (int target = 0; target < total; target++)
    {
        if (target == my_id)
        {
            continue;
        }
        thread_pool->addObjJob(this, &raft::send_request_vote, target, args);
    }

    storage->append_meta(current_term, voted_for);
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election()
{
    timeout = 300 + rand() % 201;
    role = raft_role::follower;

    while (true)
    {
        if (is_stopped())
            return;
        if (role != raft_role::leader)
        {
            unsigned long current_time = get_current_time();
            if (current_time - last_res_time > timeout)
            {
                if (role == raft_role::follower)
                {
                    timeout = 1000;
                    voted_for = -1;
                    start_election();
                }
                else
                {
                    role = raft_role::follower;
                    voted_for = -1;
                    timeout = 300 + rand() % 201;
                    storage->append_meta(current_term, voted_for);
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit()
{

    while (true)
    {
        if (is_stopped())
            return;
        // Your code here:
        if (role == raft_role::leader)
        {
            // find the majority number
            int total = num_nodes();
            std::vector<int> tmp;
            for (int i = 0; i < (int)total; i++)
            {
                tmp.push_back(match_index[i]);
            }
            sort(tmp.begin(), tmp.end());
            int N = tmp[total / 2];
            if (N > commit_idx && logs[N].term == current_term)
            {
                commit_idx = N;
            }
            append_entries_args<command> args;
            args.isHeartBeat = false;
            args.leaderCommit = commit_idx;
            args.leaderId = my_id;
            args.term = current_term;
            int lastIndex = logs.size() - 1;
            for (int target = 0; target < total; target++)
            {
                if (my_id == target)
                {
                    continue;
                }
                if (lastIndex >= next_index[target])
                {
                    int index = next_index[target] - 1;
                    args.prevLogIndex = index;
                    args.prevLogTerm = logs[index].term;
                    args.entries.erase(args.entries.begin(), args.entries.end());
                    std::vector<class log_entry<command>>().swap(args.entries);
                    args.entries.insert(args.entries.end(), logs.begin() + index + 1, logs.end());
                    int size = logs.size();
                    thread_pool->addObjJob(this, &raft::send_append_entries, target, args);
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply()
{
    // Apply committed logs the state machine
    // Work for all the nodes.

    while (true)
    {
        if (is_stopped())
            return;
        this->mtx.lock();
        if (commit_idx > last_applied)
        {
            for (int i = last_applied + 1; i <= commit_idx; i++)
            {
                state->apply_log(logs[i].commandLog);
            }
            last_applied = commit_idx;
        }
        this->mtx.unlock();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping()
{
    // Send empty append_entries RPC to the followers.
    // Only work for the leader.

    while (true)
    {
        if (is_stopped())
            return;
        // Your code here:
        if (role == raft_role::leader)
        {
            int total = num_nodes();
            int logSize = logs.size();
            append_entries_args<command> args;
            args.term = current_term;
            args.leaderId = my_id;
            args.leaderCommit = commit_idx;
            std::vector<log_entry<command>> entries;
            args.entries = entries;
            args.isHeartBeat = true;
            if (logSize == 1)
            {
                args.prevLogIndex = 0;
                args.prevLogTerm = 0;
            }
            else
            {
                args.prevLogIndex = logSize - 1;
                args.prevLogTerm = logs[logSize - 1].term;
            }

            for (int target = 0; target < total; target++)
            {
                if (target == my_id)
                {
                    continue;
                }
                thread_pool->addObjJob(this, &raft::send_append_entries, target, args);
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }
    return;
}

/******************************************************************

                        Other functions

*******************************************************************/

#endif // raft_h