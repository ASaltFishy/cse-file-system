// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <map>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {

 protected:
  int nacquire;
  struct holder{
    int client;
    pthread_t thread;
    int acquireTime;
    
  };
  std::map<lock_protocol::lockid_t,holder> lockMap;
  pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
  pthread_cond_t releasedLock = PTHREAD_COND_INITIALIZER;

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, std::string stid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, std::string stid, int &);
};

#endif 