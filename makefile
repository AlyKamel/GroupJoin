CC = g++
CCFLAGS = -std=c++11 -Iinclude
TBB_WARN = -DTBB_SUPPRESS_DEPRECATED_MESSAGES
OBJDIR = build

OBJECTS = $(OBJDIR)/testrunner.o $(OBJDIR)/tests.o $(OBJDIR)/util.o

all: CCFLAGS += -Wall -Wextra
all: groupjoin

debug: CCFLAGS += -g
debug: groupjoin

groupjoin: $(OBJDIR) $(OBJECTS)
	$(CC) $(OBJECTS) -ltbb -lpthread -o gjtest

$(OBJDIR): 
	mkdir -p $@

$(OBJDIR)/testrunner.o: src/testrunner.cpp include/tests.hpp
	$(CC) -c $< -o $@ $(TBB_WARN) $(CCFLAGS)

$(OBJDIR)/%.o: src/%.cpp include/%.hpp
	$(CC) -c $< -o $@ $(TBB_WARN) $(CCFLAGS)

clean:
	rm -rf $(OBJDIR) gjtest