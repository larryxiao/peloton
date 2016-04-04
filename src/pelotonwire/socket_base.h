//
// Created by siddharth on 31/3/16.
//

#ifndef PELOTON_SOCKET_BASE_H
#define PELOTON_SOCKET_BASE_H

#include <array>
#include <vector>
#include <thread>
#include <cstring>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>
#include <iostream>

#define SOCKET_BUFFER_SIZE 8
#define MAX_CONNECTIONS 64

namespace peloton {
namespace wire {

	class PacketManager;

	typedef unsigned char uchar;

	typedef std::array<uchar, SOCKET_BUFFER_SIZE> SockBuf;

	struct Server {
		int port;
		int server_fd;
		int max_connections;

		inline Server(int port, int max_conn) :
				port(port), max_connections(max_conn)
		{}
	};

	/*
	 * SocektManager - Wrapper for managing socket.
	 * 	B is the STL container type used as the protocol's buffer.
	 */
	template <typename B>
	class SocketManager {
		int sock_fd;
		size_t buf_ptr;
		size_t buf_size;
		SockBuf buf;

	private:
		bool refill_buffer();

	public:
		inline SocketManager(int sock_fd) : sock_fd(sock_fd),
																				buf_ptr(0), buf_size(0)
		{}

		// template used to encapsulate protocol's buffer array generics
		bool read_bytes(B& pkt_buf, size_t bytes);

		void close_socket();

	};

	extern void start_server(Server *server);

	template <typename P, typename B>
	void client_handler(int *clientfd);

	template <typename P, typename B>
	void handle_connections(Server *server);

	extern void error(const std::string &msg, bool if_exit = true);


	/*
	 * Functions defined here for template visibility
	 *
	 */

	/*
	 * handle_connections - Server's accept loop. Takes the protocol's PacketManager (P)
	 * 		and STL container type for the protocol's buffer (B)
	 */

	template <typename P, typename B>
	void handle_connections(Server *server) {
		int *clientfd, connfd, clilen;
		struct sockaddr_in cli_addr;
		clilen = sizeof(cli_addr);
		for (;;) {
			// block and wait for incoming connection
			connfd = accept(server->server_fd,
											(struct sockaddr *) &cli_addr,
											(socklen_t *) &clilen);
			if (connfd < 0){
				error("Server error: Connection not established", false);
			}

			clientfd = new int(connfd);
			std::thread client_thread(client_handler<P, B>, clientfd);
			client_thread.detach();
		}
	}


	/*
	 * client_handler - Thread function to handle a client.
	 * 		Takes the protocol's PacketManager (P) and STL container
	 * 		type for the protocol's buffer (B)
	 */
	template <typename P, typename B>
	void client_handler(int *clientfd) {
		int fd = *clientfd;
		delete clientfd;
		std::cout << "Client fd:" << fd << std::endl;
		SocketManager<B> sm(fd);
		P p(&sm);
		p.manage_packets();
	}

}
}
#endif //PELOTON_SOCKET_BASE_H
