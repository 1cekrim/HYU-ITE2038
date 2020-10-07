CC=g++

MAINDIR=main_src/
INC=include/
LIBS=lib/
TESTDIR=tests/

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

CFLAGS+= -g -fPIC -I $(INC)

TARGET=main

TEST_TARGET=test

all: $(TARGET)

$(TARGET): $(TARGET_OBJ) $(OBJS_FOR_LIB)
	make static_library
	$(CC) $(CFLAGS) -o $@ $^ -L $(LIBS) -lbpt

$(TEST_TARGET): $(OBJS_FOR_LIB) $(TEST_OBJ)
	make static_library
	$(CC) $(CFLAGS) -o $@ $^ -L $(LIBS) -lbpt

%.o: %.cc
	${CC} ${CFLAGS} -c -o $@ $<

%.o: %.c
	gcc ${CFLAGS} -c -o $@ $<

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

library:
	g++ -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
