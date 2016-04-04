//
// Created by siddharth on 31/3/16.
//

#ifndef PELOTON_SOCKET_BASE_H
#define PELOTON_SOCKET_BASE_H

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

#define SOCKET_BUFFER_SIZE 8192
#define MAX_CONNECTIONS 64

namespace peloton {
namespace wire {

	class PacketManager;

	typedef unsigned char uchar;

	
	struct Server {
		int port;
		int server_fd;
		int max_connections;

		inline Server(int port, int max_conn) :
				port(port), max_connections(max_conn)
		{}
	};

	class SocketManager {
		int sock_fd;
		size_t buf_ptr;
		size_t buf_size;
		std::vector<uchar> buf;

	private:
		bool refill_buffer();

	public:
		inline SocketManager(int sock_fd) : sock_fd(sock_fd),
																				buf_ptr(0), buf_size(0),
																				buf(SOCKET_BUFFER_SIZE, 0)
		{}
		void close();
		bool read_bytes(std::vector<uchar>& pkt_buf, size_t bytes);
	};

	extern void start_server(Server *server);

	template <typename P>
	void client_handler(int *clientfd);

	template <class P>
	void handle_connections(Server *server);

	extern void error(const std::string &msg, bool if_exit = true);


	/*
	 * Functions defined here for template visibility
	 *
	 */


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
			std::thread client_thread(client_handler<P>, clientfd);
			client_thread.detach();
		}
	}


	template <typename P>
	void client_handler(int *clientfd) {
		int fd = *clientfd;
		delete clientfd;
		std::cout << "Client fd:" << fd << std::endl;
		SocketManager sm(fd);
		P p(&sm);
		p.manage_packets();
	}

}
}
#endif //PELOTON_SOCKET_BASE_H
