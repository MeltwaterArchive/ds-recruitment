/*
 * stream_generator.cpp
 * 
 */

#include <sys/time.h>
#include <iostream>
#include <fstream>
#include <stdexcept>

#include <boost/program_options.hpp>
#include <boost/thread.hpp>

#include <zmqpp/zmqpp.hpp>

namespace po = boost::program_options;

//=====================================================================================================================

class generator_thread
{
public:
	generator_thread(
			std::string const & filename,
			zmqpp::socket_t & socket
		);
	void operator()();
	void terminate() { _running = false; }

private:
	std::string       _filename;
	zmqpp::socket_t & _socket;
	std::ifstream     _ifs;
	bool              _running;
};

class consumer_thread
{
public:
	consumer_thread(
			zmqpp::socket_t & socket
		);
	void operator()();
	void terminate() { _running = false; }

private:
	zmqpp::socket_t & _socket;
	bool              _running;

};

//=====================================================================================================================

generator_thread::generator_thread(
		std::string const & filename,
		zmqpp::socket_t & socket
	)
	:	_filename(filename)
	,	_socket(socket)
	,	_ifs(filename.c_str())
	,	_running(false)
{
	if (!_ifs.is_open())
	{
		std::string message;
		message += "unable to open data file \"";
		message += filename;
		message += "\"";

		throw std::runtime_error(message);
	}
}

void
generator_thread::operator()()
{
	size_t read_lines;
	std::string line;

	struct timeval tv;

	_running = true;

	while (_running)
	{
		std::getline(_ifs, line);

		if (!line.empty())
		{
			++read_lines;

			gettimeofday(&tv, NULL);

			// generate message;
			zmqpp::message_t message;
			message << tv.tv_sec << tv.tv_usec << line;

			_socket.send(message, true);
		}

		if (_ifs.eof())
		{
			// start again
			_ifs.close();
			_ifs.open(_filename);
		}
	}
}

consumer_thread::consumer_thread(
		zmqpp::socket_t & socket
	)
	:	_socket(socket)
{
}

void
consumer_thread::operator()()
{
	zmqpp::poller_t poller;
	poller.add(_socket);

	struct timeval tv1;
	struct timeval tv2;
	std::string    line;

	_running = true;

	size_t count = 0;

	size_t last_items = 0;
	time_t last_tv = std::time(NULL);

	double running_diff = 0;
	double time_diff    = 0;

	while (_running)
	{
		if (poller.poll(100))
		{
			if (poller.has_input(_socket))
			{
				zmqpp::message_t message;
				_socket.receive(message);

				message >> tv1.tv_sec >> tv1.tv_usec >> line;
				gettimeofday(&tv2, NULL);

				double sec_diff  = tv2.tv_sec  - tv1.tv_sec;
				double usec_diff = tv2.tv_usec - tv1.tv_usec;

				running_diff += 1000000 * sec_diff;
				running_diff += usec_diff;

				++count;
			}
			else
			{
				// eh? there is only one socket
			}
		}
		else
		{
			// timeout occurred
			gettimeofday(&tv2, NULL);
		}

		time_diff = tv2.tv_sec - last_tv;
		if ( 0 < time_diff )
		{
			double items = count - last_items;
			double ips   = items / time_diff;

			double latency = running_diff / items;

			std::cout << "\rrunning: " << ips << " items/sec, latency: " << latency << " useconds                                                                                                                    ";
			std::cout.flush();

			last_items   = count;
			last_tv      = tv2.tv_sec;
			running_diff = 0;
		}
	}
}



//=====================================================================================================================

int
main(
		int argc,
		char* argv[]
	)
{
	int result = 0;

	try
	{
		// Declare the supported options.
		po::options_description desc("Allowed options");
		desc.add_options()
			("help,h",     "produce help message")
			("input,i",  po::value<std::string>(), "set which endpoint to connect to on input side of buffer")
			("output,o", po::value<std::string>(), "set which endpoint to connect to on output side of buffer")
			("data,d",   po::value<std::string>(), "data file containing interactions to play through the buffer")
		;

		po::variables_map vm;
		po::store(po::parse_command_line(argc, argv, desc), vm);
		po::notify(vm);

		if (vm.count("help"))
		{
			std::cout << desc << std::endl;
			return 1;
		}

		std::string input_endpoint;
		std::string output_endpoint;
		std::string data_file;

		if (vm.count("input"))
		{
			input_endpoint =  vm["input"].as< std::string >();
			std::cout << "input endpoint: " << input_endpoint << std::endl;
		}
		else
		{
			std::cout << desc << std::endl;
			return 1;
		}

		if (vm.count("output"))
		{
			output_endpoint = vm["output"].as< std::string >();
			std::cout << "output endpoint: " <<  output_endpoint << std::endl;
		}
		else
		{
			std::cout << desc << std::endl;
			return 1;
		}

		if (vm.count("data"))
		{
			data_file = vm["data"].as< std::string >();
			std::cout << "data file: " <<  data_file << std::endl;
		}
		else
		{
			std::cout << desc << std::endl;
			return 1;
		}

		// create ZMQ context
		zmqpp::context_t context;

		zmqpp::socket_t input_socket(context,  zmqpp::socket_type::push);
		zmqpp::socket_t output_socket(context, zmqpp::socket_type::pull);

		input_socket.set(zmqpp::socket_option::linger,  1);
		output_socket.set(zmqpp::socket_option::linger, 1);

		std::cout << "connecting to input_socket \"" << input_endpoint << "\"" << std::endl;
		input_socket.connect(input_endpoint);

		std::cout << "connecting to output_socket \"" << output_endpoint << "\"" << std::endl;
		output_socket.connect(output_endpoint);


		generator_thread gt(data_file, input_socket);
		consumer_thread  ct(output_socket);

		std::cout << "spawning worker threads" << std::endl;

		std::cout << "type something followed by <return> to exit" << std::endl;
		boost::thread generator( boost::ref(gt) );
		boost::thread consumer( boost::ref(ct) );

		std::string dummy;
		std::cin >> dummy;

		gt.terminate();
		sleep(1);
		ct.terminate();

		generator.join();
		consumer.join();

	}
	catch(boost::thread_interrupted const &)
	{
		std::cerr << "caught boost::thread_interrupted" << std::endl;
		result = 1;
	}
	catch(std::exception const & e)
	{
		std::cerr << "caught std::exception, reason: " << e.what() << std::endl;
		result = 1;
	}
	catch(...)
	{
		std::cerr << "caught UNKNOWN exception" << std::endl;;
		result = 1;
	}

	return result;
}
