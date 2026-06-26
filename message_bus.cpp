#include "message_bus.h"

MessageBus::MessageBus(int n) : numNodes(n) {
    inboxes.resize(n);
    mtxs = vector<mutex>(n);
    cvs  = vector<condition_variable>(n);
}

void MessageBus::send(const Message& msg) {
    if (msg.to < 0 || msg.to >= numNodes) return;
    {
        lock_guard<mutex> lock(mtxs[msg.to]);
        inboxes[msg.to].push(msg);
    }
    cvs[msg.to].notify_one();
}

Message MessageBus::receive(int nodeId) {
    unique_lock<mutex> lock(mtxs[nodeId]);
    cvs[nodeId].wait(lock, [this, nodeId] {
        return !inboxes[nodeId].empty();
    });
    Message msg = inboxes[nodeId].front();
    inboxes[nodeId].pop();
    return msg;
}

bool MessageBus::tryReceive(int nodeId, Message& msg) {
    lock_guard<mutex> lock(mtxs[nodeId]);
    if (inboxes[nodeId].empty()) return false;
    msg = inboxes[nodeId].front();
    inboxes[nodeId].pop();
    return true;
}

int MessageBus::inboxSize(int nodeId) {
    lock_guard<mutex> lock(mtxs[nodeId]);
    return (int)inboxes[nodeId].size();
}
