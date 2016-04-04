//
// Created by siddharth on 31/3/16.
//

#include "socket_base.h"
#include <stdlib.h>

namespace peloton {
namespace wire {

	void start_server(Server *server) {
		struct sockaddr_in serv_addr;

		server->server_fd = socket(AF_INET, SOCK_STREAM, 0);
		if (server->server_fd < 0)
			error("Server error: while opening socket");

		memset(&serv_addr, 0, sizeof(serv_addr));

		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = INADDR_ANY;
		serv_addr.sin_port = htons(server->port);

		if (bind(server->server_fd, (struct sockaddr *) &serv_addr,
						 sizeof(serv_addr)) < 0)
			error("Server error: while binding");

		listen(server->server_fd, server->max_connections);
	}

	bool SocketManager::refill_buffer() {

		ssize_t bytes_read;

		// our buffer is to be emptied
		buf_ptr = buf_size = 0;

		// return explicitly
		for (;;) {
			//  try to fill the available space in the buffer
			bytes_read = read(sock_fd, &buf[buf_ptr],
												SOCKET_BUFFER_SIZE - buf_size );

			if (bytes_read < 0 ) {
				if ( errno == EINTR) {
					// interrupts are OK
					continue;
				}

				// otherwise, report error
				error("Socket error: could not receive data from client", false);
				return false;
			}

			if (bytes_read == 0) {
				// EOF, return
				return false;
			}

			// read success, update buffer size
			buf_size += bytes_read;

			// reset buffer ptr, to cover special case
			buf_ptr = 0;
			return true;
		}
	}

	/*
	 * read - Tries to read "bytes" bytes into packet's buffer. Returns true on success.
	 * 		false on failure.
	 */
	bool SocketManager::read_bytes(std::vector<uchar>& pkt_buf, size_t bytes) {
		size_t window;
		// while data still needs to be read
		while(bytes) {
			// how much data is available
			window = buf_size - buf_ptr;
			if (bytes <= window) {
				pkt_buf.insert(std::end(pkt_buf), std::begin(buf) + buf_ptr,
												std::begin(buf) + bytes);

				// move the pointer
				buf_ptr += bytes;

				return true;
			} else {
				// read what is available
				pkt_buf.insert(std::end(pkt_buf), std::begin(buf) + buf_ptr,
												std::end(buf));

				// update bytes leftover
				bytes -= window;

				// refill buffer, reset buf ptr here
				if (!refill_buffer()) {
					// nothing more to read, end
					return false;
				}
			}
		}

		return true;
	}

	void error(const std::string& msg, bool if_exit) {
		std::cerr << msg << std::endl;
		if (if_exit) {
			exit(EXIT_FAILURE);
		}
	}

}
}