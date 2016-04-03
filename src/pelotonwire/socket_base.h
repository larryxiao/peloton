//
// Created by siddharth on 31/3/16.
//

#ifndef PELOTON_SOCKET_BASE_H
#define PELOTON_SOCKET_BASE_H

#include <vector>
#include <thread>
#include <cstring>

namespace peloton {
namespace wire {

	struct Server {
		int port;
		int server_fd;
		int max_connections;

		inline Server(int port, int max_conn) :
				port(port), max_connections(max_conn)
		{}
	};

	extern void start_server(Server *server);

	extern void handle_connections(Server *server);

	extern void client_handler(int *clientfd);
}
}
#endif //PELOTON_SOCKET_BASE_H
