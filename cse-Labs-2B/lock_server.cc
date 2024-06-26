// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mtx);
  if(lock_cond.find(lid)==lock_cond.end()){
    lock_cond.insert(std::pair<lock_protocol::lockid_t,pthread_cond_t>(lid,PTHREAD_COND_INITIALIZER));
    lock_flag.insert(std::pair<lock_protocol::lockid_t,bool>(lid,false));  
  }else{
    while(!lock_flag[lid]){
      pthread_cond_wait(&(lock_cond.find(lid)->second),&mtx);
    }
    lock_flag[lid]=false;
  }
  pthread_mutex_unlock(&mtx);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  pthread_mutex_lock(&mtx);
  if(lock_cond.find(lid)!=lock_cond.end()){
    lock_flag[lid]=true;
    pthread_cond_signal(&(lock_cond.find(lid)->second));
  }
  pthread_mutex_unlock(&mtx);
  return ret;
}