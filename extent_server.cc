// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "extent_server.h"
#include "persister.h"

//initialize the static value
chfs_command::txid_t chfs_command::latest_id = 0;

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  std::vector<chfs_command> entries = _persister->restore_logdata();
  printf("es: log entry number:%ld\n",entries.size());
  for(auto it=entries.begin();it<entries.end();it++){
    printf("es: recover cmd type:%d\n",it->type);
    switch (it->type)
    {
    case chfs_command::CMD_CREATE:
      im->alloc_inode(it->fileType);
      break;
    case chfs_command::CMD_PUT:
      im->write_file(it->inum,it->newbuf.data(),it->newsize);
      break;
    case chfs_command::CMD_REMOVE:
      im->remove_file(it->inum);
      break;
    default:
      break;
    }
  }
}

void extent_server::beginTX(){
  chfs_command entry(chfs_command::CMD_BEGIN);
  _persister->append_log(entry);
}

void extent_server::commitTX(){
  chfs_command entry(chfs_command::CMD_COMMIT);
  _persister->append_log(entry);
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  // for log
  extent_protocol::attr attr;
  im->get_attr(id,attr);
  printf("save cmd: CREATE-%lld\n",id);
  chfs_command entry(chfs_command::CMD_CREATE,id,attr.type);
  _persister->append_log(entry);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();

  //get the old buf for persistence
  char *tempbuf = NULL;
  int oldsize;
  std::string oldbuf;
  im->read_file(id, &tempbuf, &oldsize);
  oldbuf.assign(tempbuf,oldsize);

  printf("\textent server put buf size:%ld buf:%s",buf.size(),buf.data());
  im->write_file(id, cbuf, size);
  
  printf("save cmd: PUT-%lld, oldsize-%d, newsize-%d\n",id,oldsize,size);
  chfs_command entry(chfs_command::CMD_PUT,id,oldsize,oldbuf,size,buf);
  _persister->append_log(entry);

  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;

  //get the old buf for persistence
  int oldsize;
  std::string oldbuf;
  get(id,oldbuf);
  oldsize = oldbuf.size();

  im->remove_file(id);

  extent_protocol::attr attr;
  im->get_attr(id,attr);
  printf("save cmd: RREMOVE-%lld, oldsize-%d\n",id,oldsize);
   chfs_command entry(chfs_command::CMD_REMOVE,id,attr.type,oldsize,oldbuf);
  _persister->append_log(entry);

  return extent_protocol::OK;
}

