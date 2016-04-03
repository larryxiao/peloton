//
// Created by siddharth on 31/3/16.
//

#include "socket_base.h"
#include "protocol.h"
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <netdb.h>
#include <netinet/in.h>

#define MAX_CONNECTIONS 64

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
			std::thread client_thread(client_handler, clientfd);
			client_thread.detach();
		}
	}

	void client_handler(int *clientfd) {
		int fd = *clientfd;
		delete clientfd;
		std::cout << "Client fd:" << fd << std::endl;
	}
}
}

int main(int argc, char *argv[]) {
	if (argc != 2){
		peloton::wire::error("Usage: ./wire_server [port]");
	}

	peloton::wire::Server server(atoi(argv[1]), MAX_CONNECTIONS);
	peloton::wire::start_server(&server);
	peloton::wire::handle_connections(&server);
	return 0;
}