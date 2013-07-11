CXXFLAGS =	-O2 -g -Wall -std=c++0x -fmessage-length=0 

OBJS =		stream_generator.o

LIBS =		-l boost_program_options -l boost_thread -l boost_system  -l zmq -l zmqpp 

TARGET =	stream_generator

$(TARGET):	$(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS) $(LIBS)

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
