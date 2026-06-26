CXX      = g++
CXXFLAGS = -std=c++17 -Wall -Wextra -O2
TARGET   = raft
SRCS     = main.cpp raft_node.cpp message_bus.cpp
OBJS     = $(SRCS:.cpp=.o)

all: $(TARGET)
	@echo ""
	@echo "Build successful! Run with: ./raft"
	@echo ""

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS) -lpthread

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET) $(TARGET).exe

.PHONY: all clean
