//
// Created by Siddharth Santurkar on 31/3/16.
//

#include "marshall.h"
#include <stdio.h>
#include <boost/algorithm/string.hpp>

#define PROTO_MAJOR_VERSION(x) x >> 16

namespace peloton {
namespace wire {

wiredb::Sqlite db;

/* TXN state definitions */
uchar TXN_IDLE = 'I';
uchar TXN_BLOCK = 'T';
uchar TXN_FAIL = 'E';

void print_packet(Packet* pkt) {
  if (pkt->msg_type) {
    LOG_INFO("MsgType: %d", pkt->msg_type);
  }

  LOG_INFO("Len: %zu", pkt->len);

  LOG_INFO("BufLen: %zu", pkt->buf.size());

  //  LOG_INFO("{");
  //  for (auto ele : pkt->buf) {
  //    LOG_INFO("%d,", ele);
  //  }
  //  LOG_INFO("}\n");
}

void print_uchar_vector(std::vector<uchar>& vec){
  LOG_INFO("{");
  for (auto ele : vec) {
    LOG_INFO("%d (%c)", ele, ele);
  }
  LOG_INFO("}\n");
}

/*
 * close_client - Close the socket of the underlying client
 */
void PacketManager::close_client() { client.sock->close_socket(); }

/*
 * process_startup_packet - Processes the startup packet
 * 	(after the size field of the header).
 */
bool PacketManager::process_startup_packet(Packet* pkt,
                                           ResponseBuffer& responses) {
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

  // send auth-ok
  response->msg_type = 'R';
  packet_putint(response, 0, 4);
  responses.push_back(std::move(response));

  //  if (client.dbname.empty() || client.user.empty()) {
  //    std::vector<std::pair<uchar, std::string>> error_status = {
  //        {'S', "FATAL"}, {'M', "Invalid user or database name"}};
  //    send_error_response(error_status, responses);
  //    return false;
  //  }

  send_ready_for_query(TXN_IDLE, responses);
  return true;
}

void PacketManager::put_row_desc(std::vector<wiredb::FieldInfoType> &rowdesc, ResponseBuffer &responses) {
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'T';
  packet_putint(pkt, rowdesc.size(), 2);

  for(auto &col : rowdesc) {
    packet_putstring(pkt, std::get<0>(col));
    packet_putint(pkt, 0, 4);
    packet_putint(pkt, 0, 2);
    packet_putint(pkt, std::get<1>(col), 4);
    packet_putint(pkt, std::get<2>(col), 2);
    packet_putint(pkt, -1, 4);
    // format code for text
    packet_putint(pkt, 0, 2);
  }
  responses.push_back(std::move(pkt));
}

void PacketManager::send_data_rows(std::vector<wiredb::ResType> &results, int colcount, ResponseBuffer &responses) {
  size_t numrows = results.size() / colcount;

  // 1 packet per row
  for(size_t i = 0; i < numrows; i++) {
    std::unique_ptr<Packet> pkt(new Packet());
    pkt->msg_type = 'D';
    packet_putint(pkt, colcount, 2);
    for (int j = 0; j < colcount; j++) {
      packet_putint(pkt, results[i].second.size(), 4);
      packet_putbytes(pkt, results[i].second);
    }
    responses.push_back(std::move(pkt));
  }
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

void PacketManager::put_dummy_data_row(int colcount, int start,
                                       ResponseBuffer& responses) {
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

void PacketManager::complete_command(std::string &query_type, int rows, ResponseBuffer& responses) {
  std::unique_ptr<Packet> pkt(new Packet());
  pkt->msg_type = 'C';
  std::string tag;
  if(!query_type.compare("BEGIN"))
    tag = "BEGIN";
  else if(!query_type.compare("COMMIT"))
    tag = "COMMIT";
  else if(!query_type.compare("INSERT"))
    tag = "INSERT 0 " + std::to_string(rows);
  else
    tag = query_type + " " + std::to_string(rows);
  packet_putstring(pkt, tag);

  responses.push_back(std::move(pkt));
}

/*
* put_empty_query_response - Informs the client that an empty query was sent
*/
void PacketManager::send_empty_query_response(ResponseBuffer& responses) {
  std::unique_ptr<Packet> response(new Packet());
  response->msg_type = 'I';
  responses.push_back(std::move(response));
}

/*
 * process_packet - Main switch block; process incoming packets,
 *  Returns false if the seesion needs to be closed.
 */
bool PacketManager::process_packet(Packet* pkt, ResponseBuffer& responses) {
  uchar txn_state = TXN_IDLE;
  std::string query, query_type;

  switch (pkt->msg_type) {
    case 'Q': {
      std::string q_str = packet_getstring(pkt, pkt->len);
      LOG_INFO("Query Received: %s \n", q_str.c_str());

      std::vector<std::string> queries;
      boost::split(queries, q_str, boost::is_any_of(";"));

      // just a ';' sent
      if (queries.size() == 1) {
        send_empty_query_response(responses);
        send_ready_for_query(txn_state, responses);
        return true;
      }

      // iterate till before the trivial string after the last ';'
      for (auto query = queries.begin(); query != queries.end() - 1; query++) {
        if (query->empty()) {
          send_empty_query_response(responses);
          send_ready_for_query(TXN_IDLE, responses);
          return true;
        }

        std::vector<std::string> query_tokens;
        boost::split(query_tokens, query, boost::is_any_of("\t"), boost::token_compress_on);
        std::string query_type = query_tokens[0];

        std::vector<wiredb::ResType> results;
        std::vector<wiredb::FieldInfoType> rowdesc;
        std::string errMsg;
        int rows_affected;

        int isfailed =  db.PortalExec(query->c_str(), results, rowdesc, rows_affected, errMsg);

        if(isfailed) {
          send_error_response({{'M', errMsg}}, responses);
          return true;
        }

        put_row_desc(rowdesc, responses);

        send_data_rows(results, rowdesc.size(), responses);

        complete_command(query_type, rows_affected, responses);
      }

      // send_error_response({{'M', "Syntax error"}}, responses);
      send_ready_for_query('I', responses);
      break;
    }

    case 'P': {
      std::string prep_stmt  = get_string_token(pkt);
      LOG_INFO("Prep stmt: %s", prep_stmt.c_str());
      query = get_string_token(pkt);

      std::vector<std::string> query_tokens;
      boost::split(query_tokens, query, boost::is_any_of("\t"), boost::token_compress_on);
      query_type = query_tokens[0];

      // check if we received BEGIN SQL stmt
      if(!query.compare("BEGIN")) {
        txn_state = TXN_BLOCK;
      }

      if(!query.compare("COMMIT")) {
        txn_state = TXN_IDLE;
      }

      LOG_INFO("Query: %s", query.c_str());
      int num_params = packet_getint(pkt, 2);
      LOG_INFO("NumParams: %d", num_params);

      // TODO: Finish processing for each paramater
      std::unique_ptr<Packet> response(new Packet());

      // Send Parse complete response
      response->msg_type = '1';
      responses.push_back(std::move(response));
      break;
    }

    case 'B': {
      std::string portal_name = get_string_token(pkt);
      LOG_INFO("Portal name: %s", portal_name.c_str());
      std::string prep_stmt_name = get_string_token(pkt);
      LOG_INFO("Prep stmt name: %s", prep_stmt_name.c_str());

      int param_code_count = packet_getint(pkt, 2);
      // skip paramter codes for now
      for(int i=0; i < param_code_count; i++) {
        // keep skipping 2 bytes
        packet_getint(pkt, 2);
      }

      int param_count = packet_getint(pkt, 2);
      std::vector<std::vector<uchar>> prep_parameters;

      for (int i=0; i < param_count; i++) {
        int param_len = packet_getint(pkt, 4);
        prep_parameters.push_back(packet_getbytes(pkt, param_len));
        LOG_INFO("Bind param size: %d", param_len);
        print_uchar_vector(prep_parameters[i]);
      }

      //send bind complete
      std::unique_ptr<Packet> response(new Packet());
      response->msg_type = '2';
      responses.push_back(std::move(response));
      break;
    }

    case 'D': {
      auto mode = packet_getbytes(pkt, 1);
      // mode is a single byte
      switch(mode[0]) {
        case 'S':
          LOG_INFO("PREPARED STATEMENT RECEIVED");
          break;

        case 'P':
          LOG_INFO("PORTAL STATEMENT RECEIVED");
          break;
      }

      // TODO: figure out a way for row fetching
      put_dummy_row_desc(responses);
      break;
    }

    case 'E': {
      std::string portal_name = get_string_token(pkt);

      // TODO: maintain state across B,D and E
      // send dummy data rows
      for (int i=0 ;i < 5; i++) {
        int start = 0;

        for (int i = 0; i < 5; i++) {
          put_dummy_data_row(5, start, responses);
          start += 5;
        }
      }

      // complete_command(query_type, 5, responses);
      break;
    }

    case 'S': {
      // TODO: add txn awareness
      send_ready_for_query(txn_state, responses);
      break;
    }

    case 'X':
      LOG_INFO("Closing client");
      return false;

    default:
      LOG_INFO("Packet type not supported yet: %d (%c)", pkt->msg_type, pkt->msg_type);
  }
  return true;
}

/*
 * send_error_response - Sends the passed string as an error response.
 * 		For now, it only supports the human readable 'M' message body
 */
void PacketManager::send_error_response(
    std::vector<std::pair<uchar, std::string>> error_status,
    ResponseBuffer& responses) {
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

void PacketManager::send_ready_for_query(uchar txn_status,
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

  print_packet(&pkt);
  status = process_startup_packet(&pkt, responses);
  if (!write_packets(responses, &client) || !status) {
    // close client on write failure or status failure
    close_client();
    return;
  }

  pkt.reset();
  while (read_packet(&pkt, true, &client)) {
    print_packet(&pkt);
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

int main(int argc, char* argv[]) {
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