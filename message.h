#ifndef MESSAGE_H
#define MESSAGE_H

#include <string>
using namespace std;

enum class MessageType {
    VOTE_REQUEST,
    VOTE_RESPONSE,
    HEARTBEAT,
    APPEND_ENTRIES,
    CLIENT_REQUEST,
    CLIENT_RESPONSE
};

struct Message {
    int         from;
    int         to;
    MessageType type;
    int         term;
    bool        success;
    string      key;
    string      value;

    Message(int from, int to, MessageType type, int term,
            bool success = false,
            string key   = "",
            string value = "")
        : from(from), to(to), type(type), term(term),
          success(success), key(key), value(value) {}
};

#endif
