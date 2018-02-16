#
# Created by wessim on 07/12/17.
#


.EXPORT_ALL_VARIABLES:

MODS=

CC=gcc

CFLAGS+=-g -O2 -Os -Wall
CFLAGS+=-fPIC 
CFLAGS+=-I/usr/src/asterisk/include
CFLAGS+=-D_GNU_SOURCE
CFLAGS+=-D_REENTRANT
CFLAGS+="-DAST_MODULE=\"$*\""

INSTALL=install
INSTALL_PREFIX=
ASTLIBDIR=$(INSTALL_PREFIX)/usr/lib/asterisk
MODULES_DIR=$(ASTLIBDIR)/modules

#
# MySQL stuff...  Autoconf anyone??
#
MODS+=$(shell if [ -d /usr/local/mysql/include ] || [ -d /usr/include/mysql ] || [ -d /usr/local/include/mysql ] || [ -d /opt/mysql/include ]; then echo "app_billing.so"; fi)
CFLAGS+=$(shell if [ -d /usr/local/mysql/include ]; then echo "-I/usr/local/mysql/include"; fi)
CFLAGS+=$(shell if [ -d /usr/include/mysql ]; then echo "-I/usr/include/mysql"; fi)
CFLAGS+=$(shell if [ -d /usr/local/include/mysql ]; then echo "-I/usr/local/include/mysql"; fi)
CFLAGS+=$(shell if [ -d /opt/mysql/include/mysql ]; then echo "-I/opt/mysql/include/mysql"; fi)
MLFLAGS=
MLFLAGS+=$(shell if [ -d /usr/lib64/mysql ]; then echo "-L/usr/lib64/mysql"; fi)
MLFLAGS+=$(shell if [ -d /usr/local/mysql/lib ]; then echo "-L/usr/local/mysql/lib"; fi)
MLFLAGS+=$(shell if [ -d /usr/local/lib/mysql ]; then echo "-L/usr/local/lib/mysql"; fi)
MLFLAGS+=$(shell if [ -d /opt/mysql/lib/mysql ]; then echo "-L/opt/mysql/lib/mysql"; fi)

OBJS= app_billing_databases.o app_billing.o

all: depend $(MODS)

install: all
	for x in $(MODS); do $(INSTALL) -m 755 $$x $(MODULES_DIR) ; done
	cp billing.conf /etc/asterisk/
clean:
	rm -f *.so *.o .depend

%.so : %.o
	$(CC) -shared -Xlinker -x -o $@ $<

ifneq ($(wildcard .depend),)
include .depend
endif

app_billing.so: $(OBJS)
	$(CC) -shared -Xlinker -x -o $@ $(OBJS) -lmysqlclient -lz $(MLFLAGS)

depend: .depend

.depend:
	./mkdep $(CFLAGS) `ls *.c`

