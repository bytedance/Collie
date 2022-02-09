# MIT License

# Copyright (c) 2021 ByteDance Inc. All rights reserved.
# Copyright (c) 2021 Duke University. All rights reserved.

# See LICENSE for license information


# make clean; make for non-GDR version
# make clean; GDR=1 make for GDR version
name = collie_engine
objects = main.o helper.o endpoint.o memory.o context.o
headers = helper.hpp endpoint.hpp memory.hpp context.hpp
CC = g++

CFLAGS = -O3
LDFLAGS = -libverbs -lmlx5 -lglog -lpthread -lgflags

ifdef GDR
	CFLAGS := $(CFLAGS) -DGDR
	LDFLAGS := $(LDFLAGS) -lcuda
endif

ifdef DEBUG
	CFLAGS := $(CFLAGS) -DDEBUG -g
	LDFLAGs := $(LDFLAGS) -g
	name = collie_engine_debug
endif

$(name) : $(objects)
	g++ -o $(name) $(objects) $(LDFLAGS)
	rm -rf $(objects)

$(objects) : %.o : %.cpp $(headers)
	$(CC) -c $(CFLAGS) $< -o $@

.PHONY : clean
clean:
	rm $(name)  $(objects) collie_engine_debug
