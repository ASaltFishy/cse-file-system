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
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::create,type,id);
  if(ret!=extent_protocol::OK){
    printf("extent_client: create error:%d\n",ret);
  }
  VERIFY (ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::get,eid,buf);
  if(ret!=extent_protocol::OK){
    printf("extent_client: get error:%d\n",ret);
  }
  VERIFY (ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  ret = cl->call(extent_protocol::getattr,eid,attr);
  if(ret!=extent_protocol::OK){
    printf("extent_client: getattr error:%d\n",ret);
  }
  VERIFY (ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2B part1 code goes here
  int temp = 0;
  ret = cl->call(extent_protocol::put,eid,buf,temp);
  if(ret!=extent_protocol::OK){
    printf("extent_client: put error:%d\n",ret);
  }
  VERIFY (ret == extent_protocol::OK);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int temp = 0;
  ret = cl->call(extent_protocol::remove,eid,temp);
  if(ret!=extent_protocol::OK){
    printf("extent_client: remove error:%d\n",ret);
  }
  VERIFY (ret == extent_protocol::OK);
  // Your lab2B part1 code goes here
  return ret;
}

void extent_client::beginTX(extent_protocol::extentid_t eid){
  extent_protocol::status ret = extent_protocol::OK;
  int temp = 0;
  ret = cl->call(extent_protocol::beginTx,eid,temp);
  if(ret!=extent_protocol::OK){
    printf("extent_client: beginTx error:%d\n",ret);
  }
  VERIFY (ret == extent_protocol::OK);
}

void extent_client::commitTX(extent_protocol::extentid_t eid){
  extent_protocol::status ret = extent_protocol::OK;
  int temp = 0;
  ret = cl->call(extent_protocol::commitTx,eid,temp);
  if(ret!=extent_protocol::OK){
    printf("extent_client: commitTx error:%d\n",ret);
  }
  VERIFY (ret == extent_protocol::OK);
}


