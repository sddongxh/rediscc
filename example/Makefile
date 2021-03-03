.PHONY: all clean

CPPFLAGS = -Wall -std=c++17
LIBS = -lhiredis -lhiredis_cluster

OBJ = main.o

CPPFLAGS += -I..

all: example

example: $(OBJ)
	g++ $(CPPFLAGS)  -o $@ $^ $(LIBS) 

clean:
	rm -f *.o example
