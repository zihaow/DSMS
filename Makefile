CCFLAGS = -I.
LDFLAGS = -lpthread -ldl

all: sg dsms

sg: sg.c
	gcc -o sg sg.c $(LDFLAGS)

dsms: dsms.o sqlite3.o pmessages.o list.o
	gcc -o dsms dsms.o sqlite3.o pmessages.o list.o $(LDFLAGS)

dsms.o: dsms.c
	gcc $(CCFLAGS) -c dsms.c

sqlite3.o: sqlite3.c sqlite3.h sqlite3ext.h
	gcc $(CCFLAGS) -c -w sqlite3.c

list.o: list.c list.h
	gcc $(LDFLAGS) -c -w list.c

pmessages.o: pmessages.c pmessages.h pmessages_private.h
	gcc $(LDFLAGS) -c -w pmessages.c


clean: 
	-rm dsms.o sqlite3.o list.o pmessages.o

spotless:
	-rm sg dsms

