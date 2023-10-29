CFLAGS := -std=gnu11 -Wall -g -O2 -fsanitize=thread

LDFLAGS := -lpthread -fsanitize=thread

ALL := main
all: $(ALL)
.PHONY: all

clean:
	$(RM) $(ALL)
.PHONY: clean
