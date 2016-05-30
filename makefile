
CFLAGS	= -g -Wall -DSUN
CC	= g++
CCF	= $(CC) $(CFLAGS)

H	= .
C_DIR	= .

INCDIR	= -I$(H)
LIBDIRS = -L$(C_DIR)
LIBS    = -lclientReplFs

CLIENT_OBJECTS = client.o

all:	appl replFsServer

appl:	appl.o $(C_DIR)/libclientReplFs.a
	$(CCF) -o appl appl.o $(LIBDIRS) $(LIBS)

appl.o:	appl.c client.h appl.h
	$(CCF) -c $(INCDIR) appl.c

$(C_DIR)/libclientReplFs.a:	$(CLIENT_OBJECTS)
	ar cr libclientReplFs.a $(CLIENT_OBJECTS)
	ranlib libclientReplFs.a

client.o:	client.cpp client.h
	$(CCF) -c $(INCDIR) client.cpp

replFsServer:	server.o
	$(CCF) -o replFsServer server.o $(LIBDIRS)

server.o:	server.cpp server.h
	$(CCF) -c $(INCDIR) server.cpp

clean:
	rm -f appl *.o *.a

