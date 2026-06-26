#include <iostream>
#include <thread>
#include <chrono>
#include <string>
#include <sstream>
#include "raft_node.h"
using namespace std;

void printHelp() {
    cout << "\n  Commands:\n";
    cout << "  SET <key> <value>  →  store a key-value pair\n";
    cout << "  GET <key>          →  retrieve a value\n";
    cout << "  KILL <0|1|2>       →  crash a node (watch re-election)\n";
    cout << "  STATUS             →  show all node states\n";
    cout << "  HELP               →  show this menu\n";
    cout << "  EXIT               →  shut down\n\n";
}

int main() {
    cout << "\n";
    cout << "╔══════════════════════════════════════════════╗\n";
    cout << "║   Distributed Key-Value Store — Mini Raft   ║\n";
    cout << "║          Built by Chandresh Harkhani         ║\n";
    cout << "║          P.P. Savani University 2024-28      ║\n";
    cout << "╚══════════════════════════════════════════════╝\n\n";

    // ── Shared MessageBus ──────────────────────────────
    MessageBus bus(3);

    // ── Create 3 Raft Nodes ───────────────────────────
    RaftNode node0(0, 3, bus);
    RaftNode node1(1, 3, bus);
    RaftNode node2(2, 3, bus);

    // ── Start each node in its own thread ─────────────
    thread t0([&]{ node0.run(); });
    thread t1([&]{ node1.run(); });
    thread t2([&]{ node2.run(); });

    // Wait for leader election
    cout << "\n[System] Starting nodes... waiting for leader election.\n\n";
    this_thread::sleep_for(chrono::milliseconds(1000));

    printHelp();

    // ── Interactive CLI ────────────────────────────────
    string line;
    while (true) {
        cout << "> ";
        if (!getline(cin, line)) break;
        if (line.empty()) continue;

        istringstream iss(line);
        string cmd;
        iss >> cmd;

        // Normalize to uppercase
        for (auto& c : cmd) c = toupper(c);

        if (cmd == "EXIT") {
            cout << "\n[System] Shutting down all nodes...\n";
            node0.stop();
            node1.stop();
            node2.stop();
            break;

        } else if (cmd == "HELP") {
            printHelp();

        } else if (cmd == "STATUS") {
            cout << "\n── Node Status ─────────────────────────\n";
            node0.printStatus();
            node1.printStatus();
            node2.printStatus();
            cout << "────────────────────────────────────────\n\n";

        } else if (cmd == "SET") {
            string key, value;
            iss >> key >> value;
            if (key.empty() || value.empty()) {
                cout << "  Usage: SET <key> <value>\n";
                continue;
            }
            // Send to node 0 — redirects to leader if needed
            bus.send(Message(
                99, 0,
                MessageType::CLIENT_REQUEST,
                0, false, key, value
            ));
            this_thread::sleep_for(chrono::milliseconds(300));

        } else if (cmd == "GET") {
            string key;
            iss >> key;
            if (key.empty()) {
                cout << "  Usage: GET <key>\n";
                continue;
            }
            bus.send(Message(
                99, 0,
                MessageType::CLIENT_REQUEST,
                0, false, "GET", key
            ));
            this_thread::sleep_for(chrono::milliseconds(200));

        } else if (cmd == "KILL") {
            int nodeId;
            if (!(iss >> nodeId) || nodeId < 0 || nodeId > 2) {
                cout << "  Usage: KILL <0|1|2>\n";
                continue;
            }
            if      (nodeId == 0) node0.stop();
            else if (nodeId == 1) node1.stop();
            else                  node2.stop();

            cout << "[System] Node " << nodeId
                 << " killed. Watching re-election...\n\n";
            this_thread::sleep_for(chrono::milliseconds(800));

        } else {
            cout << "  Unknown command. Type HELP for commands.\n";
        }
    }

    t0.join();
    t1.join();
    t2.join();

    cout << "\n[System] All nodes stopped. Goodbye.\n\n";
    return 0;
}
