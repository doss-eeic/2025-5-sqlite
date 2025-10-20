# --- Makefile ---
PYTHON   := $(shell which python3)
PYVER    := $(shell $(PYTHON) -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYCFG    := python$(PYVER)-config

CC       := gcc
TARGET   := pycall
SRC      := pycall.c

CFLAGS   := $(shell $(PYCFG) --cflags --embed)
LDFLAGS  := $(shell $(PYCFG) --ldflags --embed)

all: $(TARGET)

$(TARGET): $(SRC)
	$(CC) -o $@ $^ $(CFLAGS) $(LDFLAGS)

clean:
	rm -f $(TARGET)

re: clean all

.PHONY: all clean re
