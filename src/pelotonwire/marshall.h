//
// Created by siddharth on 4/4/16.
//
//	marshall.h - Helpers to marshal and unmarshal packets in Pelotonwire
//
#ifndef PELOTON_MARSHALL_H
#define PELOTON_MARSHALL_H

#include <vector>
#include <string>
#include "socket_base.h"

#define BUFFER_INIT_SIZE 100

namespace peloton {
namespace wire {

	typedef unsigned char uchar;
	typedef std::vector<uchar> PktBuf;

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

	/*
	 * Marshallers
	 */
	extern void packet_putbyte(Packet *pkt, const uchar c);

	extern void packet_putstring(Packet *pkt, std::string& str);

	extern void packet_putint(Packet *pkt, int n, int base);

	extern void packet_putcbytes(Packet *pkt, const char *b, int len);

	/*
	 * Unmarshallers
	 */
	extern int packet_getint(Packet *pkt, uchar base);

	extern std::string packet_getstring(Packet *pkt, size_t len = 0);

	extern PktBuf packet_getbytes(Packet *pkt, size_t len = 0);

	extern std::string get_string_token(Packet *pkt);


}
}
#endif //PELOTON_MARSHALL_H
