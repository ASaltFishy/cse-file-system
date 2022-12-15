#include "raft_protocol.h"

marshall &operator<<(marshall &m, const request_vote_args &args) {
    // Lab3: Your code here
    m<<args.candidateId;
    m<<args.LastLogIndex;
    m<<args.LastLogTerm;
    m<<args.term;
    return m;
}
unmarshall &operator>>(unmarshall &u, request_vote_args &args) {
    // Lab3: Your code here
    u>>args.candidateId;
    u>>args.LastLogIndex;
    u>>args.LastLogTerm;
    u>>args.term;
    return u;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply) {
    // Lab3: Your code here
    m<<reply.trueTerm;
    m<<reply.voteGranted;
    m<<reply.leader_id;
    return m;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply) {
    // Lab3: Your code here
    u>>reply.trueTerm;
    u>>reply.voteGranted;
    u>>reply.leader_id;
    return u;
}

marshall &operator<<(marshall &m, const append_entries_reply &args) {
    // Lab3: Your code here
    m<<args.term<<args.success<<args.leader_id<<args.conflictIndex<<args.conflictTerm;
    return m;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &args) {
    // Lab3: Your code here
    m>>args.term>>args.success>>args.leader_id>>args.conflictIndex>>args.conflictTerm;
    return m;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args) {
    // Lab3: Your code here
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args) {
    // Lab3: Your code here
    return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return u;
}