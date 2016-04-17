//
// Created by siddharth on 31/3/16.
//

#include "marshall.h"
#include <stdio.h>
#include <boost/algorithm/string.hpp>

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {

/* TXN state definitions */
uchar TXN_IDLE = 'I';
uchar TXN_BLOCK = 'T';
uchar TXN_FAIL = 'E';

void print_packet(Packet *pkt) {
  printf("{");
  if (pkt->msg_type) {
    printf("%d,", pkt->msg_type);
  }
  std::string size_field = std::to_string(pkt->len + 4);
  for(size_t i=0; i < size_field.length(); i++ ) {
    printf("%d,", size_field[i]);
  }
  for(auto ele : pkt->buf) {
    printf("%d,", ele);
  }
  printf("}\n");
}

/*
 * close_client - Close the socket of the underlying client
 */
void PacketManager::close_client() { client.sock->close_socket(); }

/*
 * process_startup_packet - Processes the startup packet
 * 	(after the size field of the header).
 */
bool PacketManager::process_startup_packet(
      Packet *pkt, ResponseBuffer& responses) {

  std::string token, value;
  std::unique_ptr<Packet> response(new Packet());

  int32_t proto_version = packet_getint(pkt, sizeof(int32_t));

  if (PROTO_MAJOR_VERSION(proto_version) != 3) {
    LOG_ERROR("Protocol error: Only protocol version 3 is supported.");
    exit(EXIT_FAILURE);
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
    } else {
      if (pkt->ptr >= pkt->len) break;
      value = get_string_token(pkt);
      client.cmdline_options[token] = value;
    }
  }

  response->msg_type = 'R';
  packet_putint(response, 0, 4);
  responses.push_back(std::move(response));

  if (client.dbname.empty() || client.user.empty()) {
    std::vector<std::pair<uchar, std::string>> error_status = {
        {'S', "FATAL"}, {'M', "Invalid user or database name"}};
    send_error_response(error_status, responses);
    return false;
  }

  // send auth-ok
  send_ready_for_query(TXN_IDLE, responses);
  return true;
}

/*
 * put_dummy_row_desc - Prepare a dummy row description packet
 */
void PacketManager::put_dummy_row_desc(ResponseBuffer& responses) {

  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'T';
  packet_putint(pkt, 5, 2);

  for (int i = 0; i < 5; i++) {
    auto field = "F" + std::to_string(i);
    packet_putstring(pkt, field);
    packet_putint(pkt, 0, 4);
    packet_putint(pkt, 0, 2);
    packet_putint(pkt, 0, 4);
    packet_putint(pkt, 10, 2);
    packet_putint(pkt, -1, 4);
    // format code for text
    packet_putint(pkt, 0, 2);
  }

  responses.push_back(std::move(pkt));
}

void PacketManager::put_dummy_data_row(
    int colcount, int start, ResponseBuffer& responses) {

  std::unique_ptr<Packet> pkt(new Packet());
  std::string data;
  pkt->msg_type = 'D';
  packet_putint(pkt, colcount, 2);
  for (int i = 0; i < colcount; i++) {
    data = "row" + std::to_string(start + i);

    // add 1 for null-terminator
    packet_putint(pkt, data.length() + 1, 4);

    packet_putstring(pkt, data);
  }

  responses.push_back(std::move(pkt));
}

void PacketManager::complete_command(int rows, ResponseBuffer& responses) {

  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'C';
  std::string tag = "SELECT " + std::to_string(rows);
  packet_putstring(pkt, tag);

  responses.push_back(std::move(pkt));
}

/*
 * process_packet - Main switch block; process incoming packets,
 *  Returns false if the seesion needs to be closed.
 */
bool PacketManager::process_packet(Packet *pkt, ResponseBuffer& responses) {

  switch (pkt->msg_type) {
    case 'Q': {
      std::string q_str = packet_getstring(pkt, pkt->len);
      LOG_INFO("Query Received: %s \n", q_str.c_str());

      std::vector<std::string> queries;
      boost::split(queries, q_str, boost::is_any_of(";"));

      for (auto &query : queries) {
        if (query.empty()) {
          std::unique_ptr<Packet> response(new Packet());
          response->msg_type = 'I';
          responses.push_back(std::move(response));
          send_ready_for_query(TXN_IDLE, responses);
          return true;
        }

        put_dummy_row_desc(responses);

        int start = 0;

        for (int i = 0; i < 5; i++) {
          put_dummy_data_row(5, start, responses);
          start += 5;
        }

        complete_command(5, responses);
      }

      // send_error_response({{'M', "Syntax error"}}, responses);
      send_ready_for_query('I', responses);
      break;
    }

    case 'X':
      LOG_INFO("Closing client");
      return false;

    default:
      LOG_INFO("Packet not supported yet");
  }
  return true;
}

/*
 * send_error_response - Sends the passed string as an error response.
 * 		For now, it only supports the human readable 'M' message body
 */
void PacketManager::send_error_response(
    std::vector<std::pair<uchar, std::string>> error_status,
    ResponseBuffer& responses){

  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'E';

  for (auto entry : error_status) {
    packet_putbyte(pkt, entry.first);
    packet_putstring(pkt, entry.second);
  }

  // put null terminator
  packet_putbyte(pkt, 0);

  // don't care if write finished or not, we are closing anyway
  responses.push_back(std::move(pkt));
}

void PacketManager::send_ready_for_query(
    uchar txn_status,
    ResponseBuffer& responses) {

  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'Z';

  packet_putbyte(pkt, txn_status);

  responses.push_back(std::move(pkt));
}

/*
 * PacketManager - Main wire protocol logic.
 * 		Always return with a closed socket.
 */
void PacketManager::manage_packets() {
  Packet pkt;
  ResponseBuffer responses;
  bool status;

  // fetch the startup packet
  if (!read_packet(&pkt, false, &client)) {
    close_client();
    return;
  }

  status = process_startup_packet(&pkt, responses);
  if (!write_packets(responses, &client) || !status) {
    // close client on write failure or status failure
    close_client();
    return;
  }

  pkt.reset();
  while (read_packet(&pkt, true, &client)) {
    status = process_packet(&pkt, responses);
    if (!write_packets(responses, &client) || !status) {
      // close client on write failure or status failure
      close_client();
      return;
    }
    pkt.reset();
  }
}
}
}

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cout << "Usage: ./wire_server [port]" << std::endl;
    exit(EXIT_FAILURE);
  }

  peloton::wire::Server server(atoi(argv[1]), MAX_CONNECTIONS);
  peloton::wire::start_server(&server);
  peloton::wire::handle_connections<peloton::wire::PacketManager,
                                    peloton::wire::PktBuf>(&server);
  return 0;
}