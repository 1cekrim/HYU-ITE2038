CC=gcc
CPP=g++

MAINDIR=main_src/
INC=include/
MAIN_INC=main_include/
LIBS=lib/
TESTDIR=tests/
BUILDS=builds/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(MAINDIR)main.c
TARGET_OBJ:=$(MAINDIR)main.o

TEST_SRC:=$(wildcard tests/*.cc)
TEST_OBJ:=$(TEST_SRC:.cc=.o)

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(wildcard src/*.cc)
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.cc=.o)

CFLAGS+= -g -fPIC -I $(MAIN_INC)
CPPFLAGS+= -g -std=c++17 -fPIC -I $(INC)

TARGET=main

TEST_TARGET=test

all: $(TARGET)

$(TARGET): $(TARGET_OBJ) $(OBJS_FOR_LIB)
	mkdir -p $(BUILDS)
	mkdir -p $(LIBS)
	make static_library
	$(CC) $(CFLAGS) -o $(BUILDS)$@ $(TARGET_OBJ) -L $(LIBS) -lbpt -lstdc++

$(TEST_TARGET): $(OBJS_FOR_LIB) $(TEST_OBJ)
	mkdir -p $(BUILDS)
	mkdir -p $(LIBS)
	make static_library
	$(CPP) $(CPPFLAGS) -o $(BUILDS)$@ $^ -L $(LIBS) -lbpt

%.o: %.cc
	${CPP} ${CPPFLAGS} -c -o $@ $<

%.o: %.c
	${CC} ${CFLAGS} -c -o $@ $<

clean:
	rm -f $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)* $(TEST_OBJ) $(BUILDS)*

removedb:
	rm $(BUILDS)*.db

library:
	${CPP} -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
