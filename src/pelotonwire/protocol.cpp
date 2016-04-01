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

	const std::vector<uchar>::iterator get_end_itr(Packet *pkt, int len){
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
									reinterpret_cast<uchar*>(value));
				break;

			case 2:
				std::copy(pkt->buf.begin() + pkt->ptr,get_end_itr(pkt, base),
									reinterpret_cast<char *>(value));
				value = ntohs(value);
				break;

			case 4:
				std::copy(pkt->buf.begin() + pkt->ptr, get_end_itr(pkt, base),
									reinterpret_cast<char *>(value));
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

	void error(const std::string& msg) {
		std::cerr << msg << std::endl;
		exit(EXIT_FAILURE);
	}


}
}

int main() {
	return 0;
}