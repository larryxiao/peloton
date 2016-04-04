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
#include <unordered_map>

#define BUFFER_INIT_SIZE 100

namespace peloton {
namespace  wire {

	typedef unsigned char uchar;
	typedef std::vector<uchar> PktBuf;

	struct Client {
		SocketManager<PktBuf> *sock;
		std::string dbname;
		std::string user;
		std::unordered_map<std::string, std::string> cmdline_options;

		inline Client(SocketManager<PktBuf> *sock) : sock(sock)
		{ }
	};

	struct Packet {
		PktBuf buf;
		size_t len;
		size_t ptr;
		uchar msg_type;

		// reserve buf's size as maximum packet size
		inline Packet() {
			reset();
		}

		inline void reset() {
			buf.resize(BUFFER_INIT_SIZE);
			buf.shrink_to_fit();
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

		void close_client();

	public :
		inline PacketManager(SocketManager<PktBuf> *sock) : client(sock)
		{}

		void manage_packets();
	};
}
}

#endif //PELOTON_PROTOCOL_H
