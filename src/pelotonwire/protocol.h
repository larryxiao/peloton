//
// Created by siddharth on 31/3/16.
//

#ifndef PELOTON_PROTOCOL_H
#define PELOTON_PROTOCOL_H

#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include <stdlib.h>

namespace peloton {
namespace  wire {

	typedef unsigned  char uchar;
	struct Client {
		int sockfd;
		std::string dbname;
		std::string user;
		std::string cmdline_options;

		inline Client(int sockfd) {
			this->sockfd = sockfd;
		}
	};

	struct Packet {
		std::vector<uchar> buf;
		size_t len;
		size_t ptr;
	};

	class PacketManager {

		Client client;


		int packet_getint(Packet *pkt, uchar base);

		std::string packet_getstring(Packet *pkt, size_t len = 0);

		std::vector<uchar> packet_getbytes(Packet *pkt, size_t len = 0);

	public :
		inline PacketManager(int sockfd) : client(sockfd)
		{}

		void process_startup_packet(Packet *pkt);

		void process_packet(Packet *pkt);
	};

	extern void error(const std::string &msg);

}
}

#endif //PELOTON_PROTOCOL_H
