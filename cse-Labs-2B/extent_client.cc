// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0)
  {
    printf("extent_client: bind failed\n");
  }
}

// int extent_client::tx_begin(){
//   return es->tx_begin();
// }

// void extent_client::tx_commit(uint64_t id){
//   es->tx_commit(id);
// }

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::create, type, id);
  return ret;
}

// void extent_client::tx_create(uint64_t id,uint32_t type){
//   es->tx_create(id,type);
// }

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::get, eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid,
                       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::getattr, eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int r;
  ret = cl->call(extent_protocol::put, eid, buf, r);
  return ret;
}

// void extent_client::tx_put(uint64_t id, uint64_t exid, std::string str){
//   es->tx_put(id,exid,str);
// }

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  return ret;
}

// void extent_client::tx_remove(uint64_t id, uint64_t exid){
//   es->tx_remove(id,exid);
// }
