//
// Created by Siddharth Santurkar on 31/3/16.
//

#ifndef WIRE_H
#define WIRE_H

#include "socket_base.h"
#include "sqlite.h"
#include <vector>
#include <string>
#include <iostream>
#include <unordered_map>

#define BUFFER_INIT_SIZE 100

namespace peloton {
namespace wire {

struct Packet;

typedef std::vector<uchar> PktBuf;

typedef std::vector<std::unique_ptr<Packet>> ResponseBuffer;

extern uchar TXN_IDLE, TXN_BLOCK, TXN_FAIL;

struct Client {
  SocketManager<PktBuf>* sock;
  std::string dbname;
  std::string user;
  std::unordered_map<std::string, std::string> cmdline_options;

  inline Client(SocketManager<PktBuf>* sock) : sock(sock) {}
};

struct Packet {
  PktBuf buf;
  size_t len;
  size_t ptr;
  uchar msg_type;

  // reserve buf's size as maximum packet size
  inline Packet() { reset(); }

  inline void reset() {
    buf.resize(BUFFER_INIT_SIZE);
    buf.shrink_to_fit();
    buf.clear();
    len = ptr = msg_type = 0;
  }
};

class PacketManager {
  Client client;

  void send_error_response(
      std::vector<std::pair<uchar, std::string>> error_status,
      ResponseBuffer& responses);

  void send_ready_for_query(uchar txn_status, ResponseBuffer& responses);

  void put_dummy_row_desc(ResponseBuffer& responses);

  void put_row_desc(std::vector<wiredb::FieldInfoType>& rowdesc, ResponseBuffer& responses);

  void send_data_rows(std::vector<wiredb::ResType>& results, int colcount, ResponseBuffer& responses);

  void put_dummy_data_row(int colcount, int start, ResponseBuffer& responses);

  void complete_command(std::string& query_type, int rows, ResponseBuffer& responses);

  void send_empty_query_response(ResponseBuffer& responses);

  void close_client();

 public:
  inline PacketManager(SocketManager<PktBuf>* sock) : client(sock) {}

  bool process_startup_packet(Packet* pkt, ResponseBuffer& responses);

  bool process_packet(Packet* pkt, ResponseBuffer& responses);

  void manage_packets();
};
}
}

#endif  // WIRE_H
