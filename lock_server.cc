// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server() : nacquire(0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  return ret;
}

// implementation of rerntrant lock
lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, std::string stid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2B part2 code goes here
  pthread_t tid = strtoul(stid.data(),NULL,10);
  pthread_mutex_lock(&mutex);
  if (lockMap.find(lid) == lockMap.end())
  {
    holder temp;
    temp.client = clt;
    temp.thread = tid;
    temp.acquireTime = 1;
    lockMap[lid] = temp;
  }
  else
  {
    while ((lockMap[lid].client != clt || lockMap[lid].thread != tid) && lockMap[lid].client!= -1)
    {
      pthread_cond_wait(&releasedLock, &mutex);
    }
    //lock is already hold by this thread
    if(lockMap[lid].client == clt && lockMap[lid].thread == tid){
      lockMap[lid].client = clt;
      lockMap[lid].thread = tid;
      lockMap[lid].acquireTime ++;
    }else{
      lockMap[lid].client = clt;
      lockMap[lid].thread = tid;
      lockMap[lid].acquireTime = 1;
    }
  }
  pthread_mutex_unlock(&mutex);
  r = lockMap[lid].acquireTime;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, std::string stid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_t tid = strtoul(stid.data(),NULL,10);
  // Your lab2B part2 code goes here
  pthread_mutex_lock(&mutex);
  if (lockMap.find(lid) == lockMap.end())
  {
    pthread_mutex_unlock(&mutex);
  }
  else
  {
    //ensure the lock is hold by this client
    if (lockMap[lid].client == clt && lockMap[lid].thread == tid)
    {
      //release now
      lockMap[lid].acquireTime--;
      if(lockMap[lid].acquireTime==0){
        lockMap[lid].client = -1;
        pthread_mutex_unlock(&mutex);
        pthread_cond_signal(&releasedLock);
      }
      else{
        pthread_mutex_unlock(&mutex);
      }
    }
    // this lock isn't hold by this thread, do nothing
    else{
      pthread_mutex_unlock(&mutex);
    }
  }
  return ret;
}