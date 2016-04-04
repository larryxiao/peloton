//
// Created by siddharth on 31/3/16.
//

#include "protocol.h"
#include <netinet/in.h>

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {

	void check_overflow(Packet *pkt, size_t size){
		if (pkt->ptr + size >= pkt->len)
			// overflow case, throw error
			error("Parsing error: pointer overflow for int");
	}

	PktBuf::iterator get_end_itr(Packet *pkt, int len){
		if (len == 0)
			return pkt->buf.end();
		return pkt->buf.begin() + pkt->ptr + len;
	}
	/*
	 * packet_getint -  Parse an int out of the head of the
	 * 	packet. Base bytes determines the number of bytes of integer
	 * 	we are parsing out.
	 */
	int PacketManager::packet_getint(Packet *pkt, uchar base){
		int value = 0;

		check_overflow(pkt, base);

		switch(base) {
			case 1:
				std::copy(pkt->buf.begin() + pkt->ptr, get_end_itr(pkt, base),
									reinterpret_cast<uchar*>(&value));
				break;

			case 2:
				std::copy(pkt->buf.begin() + pkt->ptr,get_end_itr(pkt, base),
									reinterpret_cast<uchar *>(&value));
				value = ntohs(value);
				break;

			case 4:
				std::copy(pkt->buf.begin() + pkt->ptr, get_end_itr(pkt, base),
									reinterpret_cast<uchar *>(&value));
				value = ntohl(value);
				break;

			default:
				error("Parsing error: Invalid int base size");
				break;
		}

		// move the pointer
		pkt->ptr += base;
		return value;
	}

	std::vector<uchar> PacketManager::packet_getbytes(Packet *pkt, size_t len) {
		std::vector<uchar> result(len);
		check_overflow(pkt, len);
		std::copy(pkt->buf.begin() + pkt->ptr, get_end_itr(pkt, len),
							result.begin());
		// move the pointer
		pkt->ptr += len;
		return result;
	}

	/*
	 * packet_getstring - parse out a string of size len.
	 * 		if len=0? parse till the end of the string
	 */
	std::string PacketManager::packet_getstring(Packet *pkt, size_t len) {
		return std::string(pkt->buf.begin() + pkt->ptr, get_end_itr(pkt, len));
	}


	/*
	 * get_string_token - used to extract a string token
	 * 		from an unsingned char vector
	 */
	std::string get_string_token(Packet *pkt) {
		auto find_itr = std::find(pkt->buf.begin() + pkt->ptr, pkt->buf.end(), 0);

		if (find_itr == pkt->buf.end()) {
			// no match? consider the remaining vector
			// as a single string and continue
			pkt->ptr = pkt->len;
			return std::string(pkt->buf.begin() + pkt->ptr, pkt->buf.end());
		}

		// continue after the found position
		pkt->ptr = find_itr - pkt->buf.begin() + 1;
		return std::string(pkt->buf.begin() + pkt->ptr, find_itr);
	}

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
		PktBuf init_pkt(initial_read_size, 0);


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
	void PacketManager::process_startup_packet(Packet *pkt) {
		std::string token;
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
			} else if (token.compare("options")) {
				if (pkt->ptr >= pkt->len) break;
				client.cmdline_options = get_string_token(pkt);
			}
		}

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

		process_startup_packet(&pkt);

		close_client();
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