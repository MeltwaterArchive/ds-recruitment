CXXFLAGS =	-O2 -g -Wall -fmessage-length=0 -std=c++0x

OBJS =		stream_generator.o

LIBS =		-l boost_program_options -l boost_thread -l zmq -l zmqpp 

TARGET =	stream_generator

$(TARGET):	$(OBJS)
	$(CXX)  -o $(TARGET) $(OBJS) $(LIBS)

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
