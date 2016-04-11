//
// Created by siddharth on 31/3/16.
//

#ifndef WIRE_H
#define WIRE_H

#include "socket_base.h"
#include <vector>
#include <string>
#include <iostream>
#include <unordered_map>

#define BUFFER_INIT_SIZE 100

namespace peloton {
namespace  wire {

	typedef std::vector<uchar> PktBuf;

	extern uchar TXN_IDLE, TXN_BLOCK, TXN_FAIL;

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

		bool read_packet(Packet *pkt, bool has_type_field);

		bool process_startup_packet(Packet *pkt);

		bool send_error_response(std::vector<std::pair<uchar, std::string>> response);

		bool send_ready_for_query(uchar txn_status);

		bool process_packet(Packet *pkt);

		bool put_dummy_row_desc();

		bool complete_command(int rows);

		void close_client();

	public :
		inline PacketManager(SocketManager<PktBuf> *sock) : client(sock)
		{}

		void manage_packets();
	};
}
}

#endif //WIRE_H
