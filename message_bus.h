#ifndef MESSAGE_BUS_H
#define MESSAGE_BUS_H

#include "message.h"
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
using namespace std;

class MessageBus {
private:
    int numNodes;
    vector<queue<Message>>      inboxes;
    vector<mutex>               mtxs;
    vector<condition_variable>  cvs;

public:
    MessageBus(int n);
    void    send(const Message& msg);
    Message receive(int nodeId);
    bool    tryReceive(int nodeId, Message& msg);
    int     inboxSize(int nodeId);
};

#endif
