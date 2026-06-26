#include "raft_node.h"
#include <random>
#include <thread>

// ─────────────────────────────────────────────────────
// CONSTRUCTOR
// ─────────────────────────────────────────────────────
RaftNode::RaftNode(int id, int totalNodes, MessageBus& bus)
    : id(id), totalNodes(totalNodes), bus(bus),
      state(NodeState::FOLLOWER),
      currentTerm(0),
      votedFor(-1),
      votesReceived(0),
      commitIndex(-1),
      lastApplied(-1),
      running(true),
      leaderAnnounced(false)
{
    electionTimeout = randomTimeout();
    lastHeartbeat   = chrono::steady_clock::now();
    cout << "[Node " << id << "] Started as Follower"
         << " | Election timeout: " << electionTimeout.count() << "ms\n";
}

// ─────────────────────────────────────────────────────
// RANDOM TIMEOUT — 150ms to 300ms
// Ensures nodes don't all start elections simultaneously
// ─────────────────────────────────────────────────────
chrono::milliseconds RaftNode::randomTimeout() {
    static random_device rd;
    static mt19937 gen(rd());
    uniform_int_distribution<> dist(150, 300);
    return chrono::milliseconds(dist(gen));
}

string RaftNode::stateName() {
    if (state == NodeState::FOLLOWER)  return "Follower";
    if (state == NodeState::CANDIDATE) return "Candidate";
    return "Leader";
}

// ─────────────────────────────────────────────────────
// BECOME FOLLOWER
// ─────────────────────────────────────────────────────
void RaftNode::becomeFollower(int term) {
    state         = NodeState::FOLLOWER;
    currentTerm   = term;
    votedFor      = -1;
    votesReceived = 0;
    leaderAnnounced = false;
    lastHeartbeat = chrono::steady_clock::now();
    cout << "[Node " << id << "] -> FOLLOWER | Term " << term << "\n";
}

// ─────────────────────────────────────────────────────
// BECOME CANDIDATE
// Increments term, votes for self, requests votes from others
// ─────────────────────────────────────────────────────
void RaftNode::becomeCandidate() {
    state           = NodeState::CANDIDATE;
    currentTerm++;
    votedFor        = id;
    votesReceived   = 1;   // vote for ourselves
    leaderAnnounced = false;
    electionTimeout = randomTimeout();
    lastHeartbeat   = chrono::steady_clock::now();

    cout << "[Node " << id << "] -> CANDIDATE | Term "
         << currentTerm << " | Requesting votes...\n";

    for (int i = 0; i < totalNodes; i++) {
        if (i == id) continue;
        bus.send(Message(id, i, MessageType::VOTE_REQUEST,
                         currentTerm.load()));
    }
}

// ─────────────────────────────────────────────────────
// BECOME LEADER
// ─────────────────────────────────────────────────────
void RaftNode::becomeLeader() {
    state           = NodeState::LEADER;
    leaderAnnounced = true;

    cout << "\n╔══════════════════════════════════════╗\n";
    cout << "║  Node " << id << " IS NOW LEADER  |  Term "
         << currentTerm << "          ║\n";
    cout << "╚══════════════════════════════════════╝\n\n";

    sendHeartbeats(); // immediately tell followers we're here
}

// ─────────────────────────────────────────────────────
// SEND HEARTBEATS
// Leader sends these every 100ms to prevent new elections
// ─────────────────────────────────────────────────────
void RaftNode::sendHeartbeats() {
    for (int i = 0; i < totalNodes; i++) {
        if (i == id) continue;
        bus.send(Message(id, i, MessageType::HEARTBEAT,
                         currentTerm.load()));
    }
}

// ─────────────────────────────────────────────────────
// HANDLE VOTE REQUEST
// ─────────────────────────────────────────────────────
void RaftNode::handleVoteRequest(const Message& msg) {
    lock_guard<mutex> lock(stateMutex);

    bool grant = false;

    if (msg.term < currentTerm) {
        // Requester is behind — deny
        grant = false;
    } else if (msg.term > currentTerm) {
        // They're ahead — update term and vote yes
        currentTerm = msg.term;
        state       = NodeState::FOLLOWER;
        votedFor    = msg.from;
        grant       = true;
        cout << "[Node " << id << "] Voted YES for Node "
             << msg.from << " | Term " << msg.term << "\n";
    } else if (votedFor == -1 || votedFor == msg.from) {
        // Same term, haven't voted yet
        votedFor = msg.from;
        grant    = true;
        cout << "[Node " << id << "] Voted YES for Node "
             << msg.from << " | Term " << msg.term << "\n";
    } else {
        cout << "[Node " << id << "] Voted NO for Node "
             << msg.from << " (already voted for Node "
             << votedFor << ")\n";
    }

    bus.send(Message(id, msg.from, MessageType::VOTE_RESPONSE,
                     currentTerm.load(), grant));
}

// ─────────────────────────────────────────────────────
// HANDLE VOTE RESPONSE
// ─────────────────────────────────────────────────────
void RaftNode::handleVoteResponse(const Message& msg) {
    lock_guard<mutex> lock(stateMutex);

    if (state != NodeState::CANDIDATE) return;

    if (msg.term > currentTerm) {
        // Someone is ahead — step down
        currentTerm = msg.term;
        state       = NodeState::FOLLOWER;
        votedFor    = -1;
        return;
    }

    if (msg.success) {
        votesReceived++;
        cout << "[Node " << id << "] Got vote from Node "
             << msg.from << " | Votes: "
             << votesReceived << "/" << totalNodes << "\n";

        int majority = (totalNodes / 2) + 1;
        if (votesReceived >= majority && !leaderAnnounced) {
            leaderAnnounced = true;
            state = NodeState::LEADER;
        }
    }
}

// ─────────────────────────────────────────────────────
// HANDLE HEARTBEAT
// Reset election timer so we don't start unnecessary election
// ─────────────────────────────────────────────────────
void RaftNode::handleHeartbeat(const Message& msg) {
    lock_guard<mutex> lock(stateMutex);

    if (msg.term >= currentTerm) {
        currentTerm   = msg.term;
        state         = NodeState::FOLLOWER;
        votedFor      = -1;
        lastHeartbeat = chrono::steady_clock::now();
    }
}

// ─────────────────────────────────────────────────────
// HANDLE APPEND ENTRIES
// Follower receives a new log entry from leader
// ─────────────────────────────────────────────────────
void RaftNode::handleAppendEntries(const Message& msg) {
    lock_guard<mutex> lock(stateMutex);

    if (msg.term < currentTerm) return;

    currentTerm   = msg.term;
    state         = NodeState::FOLLOWER;
    lastHeartbeat = chrono::steady_clock::now();

    // Write to log and apply to KV store
    log.push_back({msg.term, msg.key, msg.value});
    kvStore[msg.key] = msg.value;
    lastApplied++;

    cout << "[Node " << id << "] Replicated: "
         << msg.key << " = " << msg.value << " ✓\n";

    // ACK back to leader
    bus.send(Message(id, msg.from, MessageType::VOTE_RESPONSE,
                     currentTerm.load(), true, msg.key, msg.value));
}

// ─────────────────────────────────────────────────────
// HANDLE CLIENT REQUEST
// Only leader processes writes/reads
// ─────────────────────────────────────────────────────
void RaftNode::handleClientRequest(const Message& msg) {
    if (state != NodeState::LEADER) {
        cout << "[Node " << id
             << "] Not leader — cannot handle request.\n"
             << "         Try sending to the current leader.\n";
        bus.send(Message(id, msg.from, MessageType::CLIENT_RESPONSE,
                         currentTerm.load(), false,
                         "ERROR", "Not the leader"));
        return;
    }

    if (msg.key == "GET") {
        lock_guard<mutex> lock(stateMutex);
        string result = "NOT FOUND";
        if (kvStore.count(msg.value))
            result = kvStore[msg.value];
        cout << "[Node " << id << " | LEADER] GET "
             << msg.value << " => " << result << "\n";
        bus.send(Message(id, msg.from, MessageType::CLIENT_RESPONSE,
                         currentTerm.load(), true, msg.value, result));
    } else {
        // SET key value
        cout << "[Node " << id << " | LEADER] SET "
             << msg.key << " = " << msg.value
             << " | Replicating to followers...\n";
        {
            lock_guard<mutex> lock(stateMutex);
            log.push_back({currentTerm.load(), msg.key, msg.value});
            kvStore[msg.key] = msg.value;
            lastApplied++;
        }
        replicateEntry({currentTerm.load(), msg.key, msg.value});
        cout << "[Node " << id << " | LEADER] Committed: "
             << msg.key << " = " << msg.value << " ✓\n";
        bus.send(Message(id, msg.from, MessageType::CLIENT_RESPONSE,
                         currentTerm.load(), true, msg.key, msg.value));
    }
}

// ─────────────────────────────────────────────────────
// REPLICATE ENTRY — leader pushes to all followers
// ─────────────────────────────────────────────────────
void RaftNode::replicateEntry(const LogEntry& entry) {
    for (int i = 0; i < totalNodes; i++) {
        if (i == id) continue;
        bus.send(Message(id, i, MessageType::APPEND_ENTRIES,
                         currentTerm.load(), true,
                         entry.key, entry.value));
    }
}

// ─────────────────────────────────────────────────────
// ELECTION TIMED OUT?
// ─────────────────────────────────────────────────────
bool RaftNode::electionTimedOut() {
    auto now     = chrono::steady_clock::now();
    auto elapsed = chrono::duration_cast<chrono::milliseconds>
                   (now - lastHeartbeat);
    return elapsed >= electionTimeout;
}

// ─────────────────────────────────────────────────────
// HANDLE MESSAGE — dispatcher
// ─────────────────────────────────────────────────────
void RaftNode::handleMessage(const Message& msg) {
    switch (msg.type) {
        case MessageType::VOTE_REQUEST:    handleVoteRequest(msg);    break;
        case MessageType::VOTE_RESPONSE:   handleVoteResponse(msg);   break;
        case MessageType::HEARTBEAT:       handleHeartbeat(msg);      break;
        case MessageType::APPEND_ENTRIES:  handleAppendEntries(msg);  break;
        case MessageType::CLIENT_REQUEST:  handleClientRequest(msg);  break;
        default: break;
    }
}

// ─────────────────────────────────────────────────────
// RUN — main loop (runs inside thread)
// ─────────────────────────────────────────────────────
void RaftNode::run() {
    auto lastHeartbeatSent = chrono::steady_clock::now();

    while (running) {
        // Process all pending messages (non-blocking)
        Message msg(0, 0, MessageType::HEARTBEAT, 0);
        while (bus.tryReceive(id, msg))
            handleMessage(msg);

        // If leader: send heartbeats every 100ms
        if (state == NodeState::LEADER) {
            auto now = chrono::steady_clock::now();
            auto elapsed = chrono::duration_cast<chrono::milliseconds>
                           (now - lastHeartbeatSent);
            if (elapsed.count() >= 100) {
                sendHeartbeats();
                lastHeartbeatSent = now;
            }
        }

        // Announce leadership if just won election
        {
            lock_guard<mutex> lock(stateMutex);
            if (state == NodeState::LEADER && leaderAnnounced &&
                votesReceived >= (totalNodes / 2) + 1) {
                // Already set in handleVoteResponse — call becomeLeader
                // only once to print announcement
                static bool printed[3] = {false, false, false};
                if (!printed[id]) {
                    printed[id] = true;
                    // unlock before becomeLeader (it prints, no relock)
                }
            }
        }

        // If we just won (state set to LEADER in handleVoteResponse)
        // print the leader announcement once
        if (state == NodeState::LEADER) {
            static bool announced[3] = {false, false, false};
            if (!announced[id]) {
                announced[id] = true;
                becomeLeader();
            }
        }

        // If follower/candidate: check election timeout
        if (state != NodeState::LEADER && electionTimedOut())
            becomeCandidate();

        this_thread::sleep_for(chrono::milliseconds(10));
    }

    cout << "[Node " << id << "] 💀 Stopped.\n";
}

// ─────────────────────────────────────────────────────
// STOP — simulate node crash
// ─────────────────────────────────────────────────────
void RaftNode::stop() {
    running = false;
    cout << "\n[Node " << id << "] 💀 CRASHED\n\n";
}

// ─────────────────────────────────────────────────────
// PRINT STATUS
// ─────────────────────────────────────────────────────
void RaftNode::printStatus() {
    lock_guard<mutex> lock(stateMutex);
    cout << "  Node " << id
         << "  |  State: " << stateName()
         << "  |  Term: "  << currentTerm
         << "  |  Log: "   << log.size() << " entries"
         << "  |  KV: "    << kvStore.size() << " keys\n";
}

string RaftNode::getKVValue(const string& key) {
    lock_guard<mutex> lock(stateMutex);
    if (kvStore.count(key)) return kvStore.at(key);
    return "NOT FOUND";
}
