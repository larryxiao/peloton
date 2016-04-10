//
// Created by siddharth on 31/3/16.
//

#include "protocol.h"

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {


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
		pkt_size = ntohl(pkt_size) - initial_read_size;

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
		if (!client.sock->write_bytes(pkt->buf, pkt->len, pkt->msg_type))
			return false;

		if (client.dbname.empty() || client.user.empty()){
			std::vector<std::pair<uchar, std::string>> responses =
					{{'S', "FATAL"},{'C', "3D000"}, {'M', "Invalid user or database name"}};
			send_error_response(responses);
			return false;
		}

		// send auth-ok
		return send_ready_for_query(TXN_IDLE);
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

		// don't care if write finished or not, we are closing anyway
		return client.sock->write_bytes(pkt.buf, pkt.len, pkt.msg_type);
	}

	bool PacketManager::send_ready_for_query(uchar txn_status) {
		Packet pkt;
		pkt.msg_type = 'Z';

		packet_putbyte(&pkt, txn_status);

		return client.sock->write_bytes(pkt.buf, pkt.len, pkt.msg_type);
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
			// return;
		}

		// close_client();
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