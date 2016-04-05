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
			return pkt->buf.end();
		return pkt->buf.begin() + pkt->ptr + len;
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

		result.insert(result.end(), pkt->buf.begin() + pkt->ptr,
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
		return std::string(pkt->buf.begin() + pkt->ptr, get_end_itr(pkt, len));
	}

	/*
	 * get_string_token - used to extract a string token
	 * 		from an unsigned char vector
	 */
	std::string get_string_token(Packet *pkt) {
		// save start itr position of string
		auto start = pkt->buf.begin() + pkt->ptr;

		auto find_itr = std::find(start, pkt->buf.end(), 0);

		if (find_itr == pkt->buf.end()) {
			// no match? consider the remaining vector
			// as a single string and continue
			pkt->ptr = pkt->len;
			return std::string(pkt->buf.begin() + pkt->ptr, pkt->buf.end());
		}

		// update ptr position
		pkt->ptr = find_itr - pkt->buf.begin() + 1;

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
		str += "\0";
		pkt->buf.insert(pkt->buf.end(), str.begin(), str.end());
		pkt->len += str.size();
	}

	void packet_putint(Packet *pkt, int n, int base) {
		switch(base){
			case 2:
				n = htons(n);
				break;

			case 4:
				n = ntohl(n);
				break;

			default:
				error("Parsing error: Invalid base for int");
		}

		packet_putcbytes(pkt, reinterpret_cast<char *>(&n), base);

	}

	void packet_putcbytes(Packet *pkt, const char *b, int len) {
		std::copy(b, b + len, std::back_inserter(pkt->buf));
		pkt->len += len;
	}

}
}