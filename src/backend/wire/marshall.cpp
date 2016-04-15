//
// Created by siddharth on 4/4/16.
//

#include "marshall.h"
#include <netinet/in.h>
#include <algorithm>
#include <cstring>
#include <iterator>

namespace peloton {
namespace wire {

	void check_overflow(Packet *pkt, size_t size){
		if (pkt->ptr + size >= pkt->len)
			// overflow case, throw error
			error("Parsing error: pointer overflow for int");
	}

	PktBuf::iterator get_end_itr(Packet *pkt, int len){
		if (len == 0)
			return std::end(pkt->buf);
		return std::begin(pkt->buf) + pkt->ptr + len;
	}

	/*
	 * packet_getint -  Parse an int out of the head of the
	 * 	packet. Base bytes determines the number of bytes of integer
	 * 	we are parsing out.
	 */
	int packet_getint(Packet *pkt, uchar base){
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

	PktBuf packet_getbytes(Packet *pkt, size_t len) {
		PktBuf result;
		check_overflow(pkt, len);

		result.insert(std::end(result), std::begin(pkt->buf) + pkt->ptr,
									get_end_itr(pkt, len));

		// move the pointer
		pkt->ptr += len;
		return result;
	}

	/*
	 * packet_getstring - parse out a string of size len.
	 * 		if len=0? parse till the end of the string
	 */
	std::string packet_getstring(Packet *pkt, size_t len) {
		return std::string(std::begin(pkt->buf) + pkt->ptr, get_end_itr(pkt, len));
	}

	/*
	 * get_string_token - used to extract a string token
	 * 		from an unsigned char vector
	 */
	std::string get_string_token(Packet *pkt) {
		// save start itr position of string
		auto start = std::begin(pkt->buf) + pkt->ptr;

		auto find_itr = std::find(start, std::end(pkt->buf), 0);

		if (find_itr == std::end(pkt->buf)) {
			// no match? consider the remaining vector
			// as a single string and continue
			pkt->ptr = pkt->len;
			return std::string(std::begin(pkt->buf) + pkt->ptr, std::end(pkt->buf));
		}

		// update ptr position
		pkt->ptr = find_itr - std::begin(pkt->buf) + 1;

		// edge case
		if (start == find_itr)
			return std::string("");

		return std::string(start, find_itr);
	}

	void packet_putbyte(Packet *pkt, const uchar c) {
		pkt->buf.push_back(c);
		pkt->len++;
	}

	void packet_putstring(Packet *pkt, std::string& str) {
		pkt->buf.insert(std::end(pkt->buf), std::begin(str), std::end(str));
		// add null character
		pkt->buf.push_back(0);
		// add 1 for null character
		pkt->len += str.size() + 1;
	}

	void packet_putint(Packet *pkt, int n, int base) {
		switch(base){
			case 2:
				n = htons(n);
				break;

			case 4:
				n = htonl(n);
				break;

			default:
				error("Parsing error: Invalid base for int");
		}

		packet_putcbytes(pkt, reinterpret_cast<uchar *>(&n), base);

	}

	void packet_putcbytes(Packet *pkt, const uchar *b, int len) {
		pkt->buf.insert(std::end(pkt->buf), b, b + len);
		pkt->len += len;
	}

	bool packet_endmessage(Packet *pkt, Client *client) {
		return client->sock->write_bytes(pkt->buf, pkt->len, pkt->msg_type);
	}

}
}