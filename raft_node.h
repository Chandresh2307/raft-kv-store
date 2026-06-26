#ifndef RAFT_NODE_H
#define RAFT_NODE_H

#include "message_bus.h"
#include <map>
#include <vector>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <optional>
#include <iostream>
using namespace std;

enum class NodeState { FOLLOWER, CANDIDATE, LEADER };

struct LogEntry {
    int    term;
    string key;
    string value;
};

class RaftNode {
private:
    // ── Identity ──────────────────────────────────────
    int         id;
    int         totalNodes;
    MessageBus& bus;

    // ── Raft State ────────────────────────────────────
    NodeState   state;
    atomic<int> currentTerm;
    int         votedFor;
    int         votesReceived;

    // ── Log ───────────────────────────────────────────
    vector<LogEntry> log;
    int         commitIndex;
    int         lastApplied;

    // ── KV Store ──────────────────────────────────────
    map<string,string> kvStore;

    // ── Timing ────────────────────────────────────────
    chrono::milliseconds              electionTimeout;
    chrono::steady_clock::time_point  lastHeartbeat;

    // ── Thread Safety ─────────────────────────────────
    mutex       stateMutex;
    atomic<bool> running;
    bool        leaderAnnounced;  // prevent repeated leader print

    // ── Private Methods ───────────────────────────────
    void becomeFollower(int term);
    void becomeCandidate();
    void becomeLeader();

    void handleMessage(const Message& msg);
    void handleVoteRequest(const Message& msg);
    void handleVoteResponse(const Message& msg);
    void handleHeartbeat(const Message& msg);
    void handleAppendEntries(const Message& msg);
    void handleClientRequest(const Message& msg);

    void sendHeartbeats();
    void replicateEntry(const LogEntry& entry);

    bool                 electionTimedOut();
    chrono::milliseconds randomTimeout();
    string               stateName();

public:
    RaftNode(int id, int totalNodes, MessageBus& bus);

    void   run();
    void   stop();
    void   printStatus();
    string getKVValue(const string& key);
};

#endif
