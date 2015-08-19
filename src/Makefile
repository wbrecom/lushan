include common.mk

CFLAGS := -I$(LIBEVENT_HOME)/include

LIBS := $(LIBS) -L$(LIBEVENT_HOME)/lib -levent -lpthread

### Complie Rules. ###
.c.o:
	$(CC) $(CFLAGS) $(CPPFLAGS) -c -o $@ $< -fPIC
.cc.o:
	$(CXX) $(CFLAGS) $(CPPFLAGS) -c -o $@ $< -fPIC

TARGET = hyper_dict index_sort

### Objects. ###
OBJS1 = hdict.o hyper_dict.o
OBJS2 = index_sort.o

all : $(TARGET)

hyper_dict : $(OBJS1)
	$(CC) $(CFLAGS) -o $@ $(OBJS1) $(LIBS)

index_sort : $(OBJS2)
	$(CC) $(CFLAGS) -o $@ $(OBJS2) $(LIBS)


### Clean. ###
clean:
	rm -f $(TARGET) *.o *.a tags
