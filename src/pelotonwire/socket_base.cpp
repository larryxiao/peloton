//
// Created by siddharth on 31/3/16.
//

#include "socket_base.h"
#include <stdlib.h>

namespace peloton {
namespace wire {

	void start_server(Server *server) {
		struct sockaddr_in serv_addr;
		int yes = 1;

		server->server_fd = socket(AF_INET, SOCK_STREAM, 0);
		if (server->server_fd < 0)
			error("Server error: while opening socket");


		if (setsockopt(server->server_fd, SOL_SOCKET, SO_REUSEADDR, &yes,
									 sizeof(yes)) == -1) {
			error("Setsockopt error: can't config reuse addr");
		}

		memset(&serv_addr, 0, sizeof(serv_addr));

		serv_addr.sin_family = AF_INET;
		serv_addr.sin_addr.s_addr = INADDR_ANY;
		serv_addr.sin_port = htons(server->port);

		if (bind(server->server_fd, (struct sockaddr *) &serv_addr,
						 sizeof(serv_addr)) < 0)
			error("Server error: while binding");

		listen(server->server_fd, server->max_connections);
	}

	template <typename B>
	bool SocketManager<B>::refill_read_buffer() {

		ssize_t bytes_read;

		// our buffer is to be emptied
		rbuf.reset();

		// return explicitly
		for (;;) {
			//  try to fill the available space in the buffer
			bytes_read = read(sock_fd, &rbuf.buf[rbuf.buf_ptr],
												SOCKET_BUFFER_SIZE - rbuf.buf_size );
			std::cout << "Bytes Read:" << bytes_read << std::endl;
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
			rbuf.buf_size += bytes_read;

			// reset buffer ptr, to cover special case
			rbuf.buf_ptr = 0;
			return true;
		}
	}

	template <typename B>
	bool SocketManager<B>::write_socket() {
		ssize_t written_bytes = 0;
		wbuf.buf_ptr = 0;
		// still outstanding bytes
		while (wbuf.buf_size - written_bytes > 0) {

			written_bytes = write(sock_fd, &wbuf.buf[wbuf.buf_ptr], wbuf.buf_size);
			if (written_bytes < 0) {
				if (errno == EINTR) {
					// interrupts are ok, try again
					continue;
				} else {
					// fatal errors
					return false;
				}
			}

			// weird edge case?
			if (written_bytes == 0 && wbuf.buf_size !=0) {
				// fatal
				return false;
			}

			// update bookkeping
			wbuf.buf_ptr += written_bytes;
			wbuf.buf_size -= written_bytes;
		}

		// buffer is empty
		wbuf.buf_ptr = 0;
		wbuf.buf_size = wbuf.get_max_size();

		// we are ok
		return true;
	}

	/*
	 * read - Tries to read "bytes" bytes into packet's buffer. Returns true on success.
	 * 		false on failure. B can be any STL container.
	 */
	template <typename B>
	bool SocketManager<B>::read_bytes(B& pkt_buf, size_t bytes) {
		size_t window, pkt_buf_idx = 0;
		// while data still needs to be read
		while(bytes) {
			// how much data is available
			window = rbuf.buf_size - rbuf.buf_ptr;
			if (bytes <= window) {
				pkt_buf.insert(std::end(pkt_buf), std::begin(rbuf.buf) + rbuf.buf_ptr,
											 std::begin(rbuf.buf) + rbuf.buf_ptr + bytes);

				// move the pointer
				rbuf.buf_ptr += bytes;

				// move pkt_buf_idx as well
				pkt_buf_idx += bytes;

				return true;
			} else {
				// read what is available for non-trivial window
				if (window > 0) {
					pkt_buf.insert(std::end(pkt_buf), std::begin(rbuf.buf) + rbuf.buf_ptr,
												 std::begin(rbuf.buf) + rbuf.buf_size);

					// update bytes leftover
					bytes -= window;

					// update pkt_buf_idx
					pkt_buf_idx += window;
				}

				// refill buffer, reset buf ptr here
				if (!refill_read_buffer()) {
					// nothing more to read, end
					return false;
				}
			}
		}

		return true;
	}

	template <typename B>
	bool SocketManager<B>::write_bytes(B &pkt_buf, size_t len, uchar type) {
		size_t window, pkt_buf_ptr = 0;
		int len_nb; // length in network byte order
		// reset write buffer
		wbuf.reset();

		wbuf.buf_size = wbuf.get_max_size();

		// assuming wbuf is large enough to
		// fit type and size fields in one go
		if (type != 0) {
			// type shouldn't be ignored
			wbuf.buf[wbuf.buf_ptr++] = type;
		}

		// make len include its field size as well
		len_nb = htonl(len + sizeof(int32_t));

		std::copy(reinterpret_cast<uchar*>(&len_nb), reinterpret_cast<uchar *>(&len_nb) + 4,
							std::begin(wbuf.buf) + wbuf.buf_ptr);

		wbuf.buf_ptr += sizeof(int32_t);

		// fill the contents
		while(len) {
			window = wbuf.buf_size - wbuf.buf_ptr;

			if (len <= window) {
				// contents fit in window
				std::copy(std::begin(pkt_buf) + pkt_buf_ptr,
									std::begin(pkt_buf) + pkt_buf_ptr + len,
									std::begin(wbuf.buf) + wbuf.buf_ptr);
				wbuf.buf_ptr += len;
				wbuf.buf_size = wbuf.buf_ptr;
				return write_socket();
			} else {
				// non-trivial window
				std::copy(std::begin(pkt_buf) + pkt_buf_ptr,
									std::begin(pkt_buf) + pkt_buf_ptr + window,
									std::begin(wbuf.buf) + wbuf.buf_ptr);
				pkt_buf_ptr += window;
				len -= window;

				// write failure
				if(!write_socket())
					return false;
			}
		}
		return true;
	}

	template <typename B>
	void SocketManager<B>::close_socket() {
		for (;;) {
			int status = close(sock_fd);
			if (status < 0) {
				// failed close
				if (errno == EINTR) {
					// interrupted, try closing again
					continue;
				}
			}
			return;
		}
	}

	void error(const std::string& msg, bool if_exit) {
		std::cerr << msg << std::endl;
		if (if_exit) {
			exit(EXIT_FAILURE);
		}
	}

	// explicit template instantiation for read_bytes
	template class SocketManager<std::vector<uchar>>;
}
}