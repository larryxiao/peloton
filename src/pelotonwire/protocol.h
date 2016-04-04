//
// Created by siddharth on 31/3/16.
//

#ifndef PELOTON_PROTOCOL_H
#define PELOTON_PROTOCOL_H

#include "socket_base.h"
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>

namespace peloton {
namespace  wire {

	typedef unsigned char uchar;

	struct Client {
		SocketManager *sock;
		std::string dbname;
		std::string user;
		std::string cmdline_options;

		inline Client(SocketManager *sock) : sock(sock)
		{ }
	};

	struct Packet {
		std::vector<uchar> buf;
		size_t len;
		size_t ptr;
		uchar msg_type;

		inline Packet() {
			reset();
		}

		inline void reset() {
			buf.clear();
			len = ptr = msg_type = 0;
		}
	};


	class PacketManager {

		Client client;

		int packet_getint(Packet *pkt, uchar base);

		std::string packet_getstring(Packet *pkt, size_t len = 0);

		std::vector<uchar> packet_getbytes(Packet *pkt, size_t len = 0);

		bool read_packet(Packet *pkt, bool has_type_field);

		void process_startup_packet(Packet *pkt);

		void process_packet(Packet *pkt);

	public :
		inline PacketManager(SocketManager *sock) : client(sock)
		{}

		void manage_packets();
	};
}
}

#endif //PELOTON_PROTOCOL_H
