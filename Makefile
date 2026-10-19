CC      ?= cc
CFLAGS  ?= -std=c11 -D_POSIX_C_SOURCE=200809L -Wall -Wextra -O2 -pthread -Icommon
LDFLAGS ?= -pthread

ifeq ($(STATIC),1)
LDFLAGS += -static
endif

COMMON = common/net.c common/protocol.c
CLIENT = client/client
SERVER = server/server

all: $(CLIENT) $(SERVER)

$(CLIENT): client/client.c $(COMMON)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

$(SERVER): server/server.c $(COMMON)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(CLIENT) $(SERVER)

.PHONY: all clean
