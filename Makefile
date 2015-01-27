CCFLAGS = -I.
LDFLAGS = -lpthread -ldl

all: sg dsms

sg: sg.o sqlite3.o
	gcc -o sg sg.o sqlite3.o $(LDFLAGS)

dsms: dsms.o sqlite3.o
	gcc -o dsms dsms.o sqlite3.o $(LDFLAGS)

sg.o: sg.c
	gcc $(CCFLAGS) -c sg.c

dsms.o: dsms.c
	gcc $(CCFLAGS) -c dsms.c

sqlite3.o: sqlite3.c sqlite3.h sqlite3ext.h
	gcc $(CCFLAGS) -c sqlite3.c

clean:
	-rm sg.o dsms.o sqlite3.o

spotless: clean
	-rm sg dsms

