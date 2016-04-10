//
// Created by siddharth on 31/3/16.
//

#ifndef PELOTON_PROTOCOL_H
#define PELOTON_PROTOCOL_H

#include "marshall.h"
#include "socket_base.h"
#include <vector>
#include <string>
#include <iostream>
#include <unordered_map>

namespace peloton {
namespace  wire {

	uchar TXN_IDLE = 'I';
	uchar TXN_BLOCK = 'T';
	uchar TXN_FAIL = 'E';

	struct Client {
		SocketManager<PktBuf> *sock;
		std::string dbname;
		std::string user;
		std::unordered_map<std::string, std::string> cmdline_options;

		inline Client(SocketManager<PktBuf> *sock) : sock(sock)
		{ }
	};


	class PacketManager {

		Client client;

		bool read_packet(Packet *pkt, bool has_type_field);

		bool process_startup_packet(Packet *pkt);

		bool send_error_response(std::vector<std::pair<uchar, std::string>> response);

		bool send_ready_for_query(uchar txn_status);

		bool process_packet(Packet *pkt);

		void close_client();

	public :
		inline PacketManager(SocketManager<PktBuf> *sock) : client(sock)
		{}

		void manage_packets();
	};
}
}

#endif //PELOTON_PROTOCOL_H
