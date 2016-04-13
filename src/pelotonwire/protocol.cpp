//
// Created by siddharth on 31/3/16.
//

#include "marshall.h"
#include <boost/algorithm/string.hpp>

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {

	/* TXN state definitions */
	uchar TXN_IDLE = 'I';
	uchar TXN_BLOCK = 'T';
	uchar TXN_FAIL = 'E';

	/*
	 * read_packet - Tries to read a single packet, returns true on success,
	 * 		false on failure. Accepts pointer to an empty packet, and if the
	 * 		expected packet contains a type field. The function does a preliminary
	 * 		read to fetch the size value and then reads the rest of the packet.
	 *
	 * 		Assume: Packet length field is always 32-bit int
	 */

	bool PacketManager::read_packet(Packet *pkt, bool has_type_field) {
		uint32_t pkt_size = 0, initial_read_size = sizeof(int32_t);

		if (has_type_field)
			// need to read type character as well
			initial_read_size++;

		// reads the type and size of packet
		PktBuf init_pkt;

		// read first size_field_end bytes
		if(!client.sock->read_bytes(init_pkt, static_cast<size_t >(initial_read_size))) {
			// nothing more to read
			return false;
		}

		if (has_type_field) {
			// packet includes type byte as well
			pkt->msg_type = init_pkt[0];

			// extract packet size
			std::copy(init_pkt.begin() + 1, init_pkt.end(),
													 reinterpret_cast<uchar *>(&pkt_size));
		} else {
			// directly extract packet size
			std::copy(init_pkt.begin(), init_pkt.end(),
								reinterpret_cast<uchar *>(&pkt_size));
		}

		// packet size includes initial bytes read as well
		pkt_size = ntohl(pkt_size) - sizeof(int32_t);

		if (!client.sock->read_bytes(pkt->buf, static_cast<size_t >(pkt_size))) {
			// nothing more to read
			return false;
		}

		pkt->len = pkt_size;

		return true;
	}

	/*
	 * close_client - Close the socket of the underlying client
	 */
	void PacketManager::close_client() {
		client.sock->close_socket();
	}

	/*
	 * process_startup_packet - Processes the startup packet
	 * 	(after the size field of the header).
	 */
	bool PacketManager::process_startup_packet(Packet *pkt) {
		std::string token, value;
		int32_t proto_version = packet_getint(pkt, sizeof(int32_t));

		if(PROTO_MAJOR_VERSION(proto_version) != 3){
			error("Protocol error: Only protocol version 3 is supported.");
		}

		// TODO: check for more malformed cases
		// iterate till the end
		for (;;) {
			// loop end case?
			if (pkt->ptr >= pkt->len) break;
			token = get_string_token(pkt);

			// if the option database was found
			if (token.compare("database") == 0) {
				// loop end?
				if (pkt->ptr >= pkt->len) break;
				client.dbname = get_string_token(pkt);
			} else if (token.compare(("user")) == 0) {
				// loop end?
				if (pkt->ptr >= pkt->len) break;
				client.user = get_string_token(pkt);
			} else {
				if (pkt->ptr >= pkt->len) break;
				value = get_string_token(pkt);
				client.cmdline_options[token] = value;
			}
		}

		pkt->reset();
		pkt->msg_type = 'R';
		packet_putint(pkt, 0, 4);
		if (!packet_endmessage(pkt, &client))
			return false;

		if (client.dbname.empty() || client.user.empty()){
			std::vector<std::pair<uchar, std::string>> responses =
					{{'S', "FATAL"}, {'M', "Invalid user or database name"}};
			send_error_response(responses);
			return false;
		}

		// send auth-ok
		return send_ready_for_query(TXN_IDLE);
	}

	/*
	 * put_dummy_row_desc - Prepare a dummy row description packet
	 */
	bool PacketManager::put_dummy_row_desc() {
		Packet pkt;
		pkt.reset();
		pkt.msg_type = 'T';
		packet_putint(&pkt, 5, 2);
		for (int i=0; i < 5; i++) {
			auto field = "F" + std::to_string(i);
			packet_putstring(&pkt, field);
			packet_putint(&pkt, 0, 4);
			packet_putint(&pkt, 0, 2);
			packet_putint(&pkt, 0, 4);
			packet_putint(&pkt, 10, 2);
			packet_putint(&pkt, -1, 4);
			// format code for text
			packet_putint(&pkt, 0, 2);
		}

		return packet_endmessage(&pkt, &client);
	}

	bool PacketManager::put_dummy_data_row(int colcount, int start) {
		Packet pkt;
		std::string data;
		pkt.reset();
		pkt.msg_type = 'D';
		packet_putint(&pkt, colcount, 2);
		for (int i=0; i < colcount; i++) {
			data = "row" + std::to_string(start+i);

			// add 1 for null-terminator
			packet_putint(&pkt, data.length() + 1, 4);

			packet_putstring(&pkt, data);
		}

		return packet_endmessage(&pkt, &client);
	}

	bool PacketManager::complete_command(int rows) {
		Packet pkt;
		pkt.reset();
		pkt.msg_type = 'C';
		std::string tag = "SELECT " + std::to_string(rows);
		packet_putstring(&pkt, tag);

		return packet_endmessage(&pkt, &client);
	}

	/*
	 * process_packet - Main switch block; process incoming packets
	 */
	bool PacketManager::process_packet(Packet *pkt) {
		Packet resp;
		switch(pkt->msg_type){
			case 'Q':
			{
				std::string q_str = packet_getstring(pkt, pkt->len);
				std::cout << "Query Received: " << q_str << std::endl;

				std::vector<std::string> queries;
				boost::split(queries, q_str, boost::is_any_of(";"));

				for (auto &query : queries) {
					if (query.empty()) {
						resp.reset();
						resp.msg_type = 'I';
						if (!packet_endmessage(&resp, &client))
							return false;
						if (!send_ready_for_query(TXN_IDLE))
							return false;
					}

					if (!put_dummy_row_desc())
						return false;

					int start = 0;

					for (int i = 0; i < 5; i++) {
						put_dummy_data_row(5, start);
						start += 5;
					}

					if(!complete_command(5))
						return false;
				}

				// send_error_response({{'M', "Syntax error"}});
				return send_ready_for_query('I');

			}

			case 'X':
			std::cout << "Closing client: ";
				return false;

			default:
				std::cout << "Packet not supported yet" << std::endl;
		}
		return true;
	}

	/*
	 * send_error_response - Sends the passed string as an error response.
	 * 		For now, it only supports the human readable 'M' message body
	 */
	bool PacketManager::send_error_response(
			std::vector<std::pair<uchar, std::string>> responses) {
		Packet pkt;
		pkt.msg_type = 'E';

		for(auto entry : responses) {
			packet_putbyte(&pkt, entry.first);
			packet_putstring(&pkt, entry.second);
		}

		// put null terminator
		packet_putbyte(&pkt, 0);

		// don't care if write finished or not, we are closing anyway
		return packet_endmessage(&pkt, &client);
	}

	bool PacketManager::send_ready_for_query(uchar txn_status) {
		Packet pkt;
		pkt.msg_type = 'Z';

		packet_putbyte(&pkt, txn_status);

		return packet_endmessage(&pkt, &client);
	}

	/*
	 * PacketManager - Main wire protocol logic.
	 * 		Always return with a closed socket.
	 */
	void PacketManager::manage_packets() {
		Packet pkt;

		// fetch the startup packet
		if (!read_packet(&pkt, false)) {
			close_client();
			return;
		}

		if (!process_startup_packet(&pkt)) {
			close_client();
			return;
		}

		pkt.reset();
		while(read_packet(&pkt, true)) {
			if (!process_packet(&pkt)) {
				close_client();
				return;
			}
			pkt.reset();
		}
	}

}
}

int main(int argc, char *argv[]) {
	if (argc != 2){
		peloton::wire::error("Usage: ./wire_server [port]");
	}

	peloton::wire::Server server(atoi(argv[1]), MAX_CONNECTIONS);
	peloton::wire::start_server(&server);
	peloton::wire::handle_connections<peloton::wire::PacketManager, peloton::wire::PktBuf>(&server);
	return 0;
}