
# Distributed Key-Value Store — Mini Raft

A distributed key-value store built in C++17 implementing the **Raft consensus algorithm** — the same algorithm used in production systems like etcd (Kubernetes), CockroachDB, and TiKV.

Built by **Chandresh Harkhani** — B.Tech CSE, P.P. Savani University (2024–28)

---

## What is Raft?

Raft is a consensus algorithm that makes multiple servers agree on every decision — even when some servers crash. It guarantees:

- **One leader** at all times handles all writes
- **All nodes store identical data** — if the leader dies, no data is lost
- **Automatic re-election** — a new leader is elected in under 150ms after a crash

---

## Architecture

```
         CLIENT
           │
           ▼
    ┌─────────────┐     heartbeat/100ms
    │   LEADER    │ ─────────────────────► Followers stay alive
    │   (Node X)  │
    └──────┬──────┘
           │  APPEND_ENTRIES (replicate write)
           ├──────────────────► Node A: "Confirmed ✓"
           └──────────────────► Node B: "Confirmed ✓"
           │
           │  Majority (2/3) confirmed
           ▼
    Commits to log → replies to client

─────────────── NODE X CRASHES ───────────────────────

    Remaining nodes notice no heartbeat for 300ms
    Random timeout fires → one node becomes Candidate
    Gets majority votes → becomes new LEADER
    System back online in < 150ms
```

---

## Node State Machine

```
              ┌────────────┐
  start ─────►│  FOLLOWER  │◄──────────────────────┐
              └─────┬──────┘                        │
                    │ timeout — no heartbeat         │
                    ▼                          higher term
              ┌────────────┐                   discovered
              │ CANDIDATE  │──────────────────────► │
              └─────┬──────┘
                    │ majority votes (2/3)
                    ▼
              ┌────────────┐
              │   LEADER   │──── heartbeat every 100ms
              └────────────┘
```

---

## Features

| Feature | Details |
|---|---|
| Leader Election | Randomized timeouts (150–300ms) prevent split votes |
| Log Replication | Leader replicates every SET to all followers before committing |
| Fault Tolerance | New leader elected automatically when current leader crashes |
| MessageBus | Thread-safe per-node inbox using mutex + condition_variable |
| Interactive CLI | SET, GET, KILL, STATUS commands |
| Term Tracking | atomic<int> term counter — stale leaders step down instantly |

---

## File Structure

```
raft-kv-store/
├── message.h          # Message struct + MessageType enum
├── message_bus.h/cpp  # Thread-safe inter-node communication
├── raft_node.h/cpp    # Raft state machine (election + replication)
├── main.cpp           # CLI + node wiring
├── Makefile           # Build script
└── README.md
```

---

## How to Build & Run

**Windows (MinGW / VS Code):**
```bash
g++ -std=c++17 -Wall main.cpp raft_node.cpp message_bus.cpp -o raft -lpthread
./raft.exe
```

**Linux / Mac:**
```bash
make
./raft
```

---

## Demo Script

```
> STATUS
  Node 0  |  State: Follower   |  Term: 1  |  Log: 0 entries
  Node 1  |  State: Leader     |  Term: 1  |  Log: 0 entries
  Node 2  |  State: Follower   |  Term: 1  |  Log: 0 entries

> SET name Chandresh
[Node 1 | LEADER] SET name = Chandresh | Replicating...
[Node 0] Replicated: name = Chandresh ✓
[Node 2] Replicated: name = Chandresh ✓
[Node 1 | LEADER] Committed: name = Chandresh ✓

> KILL 1
[Node 1] 💀 CRASHED
[Node 0] -> CANDIDATE | Term 2 | Requesting votes...
[Node 2] Voted YES for Node 0 | Term 2
★ Node 0 IS NOW LEADER | Term 2 ★

> GET name
[Node 0 | LEADER] GET name => Chandresh   ← data survived the crash!
```

---

## Key Implementation Details

**Why randomized timeouts?**
If all nodes had the same timeout, they'd all start elections simultaneously and keep tying. Randomizing (150–300ms) ensures one node almost always starts slightly before others → wins election cleanly.

**Why MessageBus instead of TCP sockets?**
The MessageBus (thread-safe queue per node) provides identical semantics to gRPC/TCP but without 200+ lines of networking boilerplate — isolating the consensus logic cleanly. Same design used in production systems where transport is abstracted from application logic.

**Why `atomic<int>` for term?**
The term counter is read by all 3 threads simultaneously. `atomic<int>` makes read/write indivisible without needing a mutex — faster and cleaner for a single integer.

---

## Concepts Demonstrated

| Concept | Used For |
|---|---|
| `std::thread` | One thread per Raft node |
| `std::mutex` + `lock_guard` | Protecting KV store, log, state |
| `std::condition_variable` | Node sleeping until messages arrive |
| `std::atomic<int>` | Term number — safe cross-thread reads |
| `std::unique_lock` | Required by condition_variable |
| Raft consensus | Leader election + log replication |
| State machine | FOLLOWER → CANDIDATE → LEADER transitions |

---

## What I Learned

- How distributed consensus works and why it's hard (split-brain, stale leaders)
- C++17 multithreading: threads, mutex, condition_variable, atomic
- Why randomized timeouts solve the split-vote problem
- How production systems (etcd, CockroachDB) abstract transport from consensus logic
- The difference between appending to a log and committing (why the two-step matters)

---

## References

- [The Raft Paper](https://raft.github.io/raft.pdf) — Ongaro & Ousterhout (2014)
- [Raft Visualization](https://raft.github.io/) — interactive demo

---

## Tech Stack

- Language: **C++17**
- Concurrency: **std::thread, mutex, condition_variable, atomic**
- Build: **g++ / Makefile**
- OS: Windows (MinGW) / Linux / Mac
