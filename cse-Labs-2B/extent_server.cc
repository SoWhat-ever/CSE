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

extent_server::extent_server()
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here

  // Your code here for Lab2A: recover data on startup
  restore();
}

void extent_server::restore()
{
  std::vector<chfs_command> tx;
  std::vector<chfs_command> cmd = _persister->restore_logdata();
  bool iftx = false;
  while (!cmd.empty())
  {
    chfs_command tmp = cmd.front();
    if (tmp.type == chfs_command::CMD_BEGIN)
    {
      tx.clear();
      iftx = true;
      cmd.erase(cmd.begin());
      continue;
    }
    if (tmp.type == chfs_command::CMD_COMMIT)
    {
      while (!tx.empty())
      {
        chfs_command txcmd = tx.front();
        switch (txcmd.type)
        {
        case chfs_command::CMD_CREATE:
        {
          extent_protocol::extentid_t id = 0;
          create(txcmd.exid, id);
          break;
        }
        case chfs_command::CMD_PUT:
        {
          int num = 0;
          put(txcmd.exid, txcmd.content, num);
          break;
        }
        case chfs_command::CMD_REMOVE:
        {
          int num = 0;
          remove(txcmd.exid, num);
          break;
        }
        default:
        {
          break;
        }
        }
        tx.erase(tx.begin());
      }
      iftx = false;
      cmd.erase(cmd.begin());
      continue;
    }
    if (iftx)
    {
      tx.push_back(tmp);
      cmd.erase(cmd.begin());
      continue;
    }
    else
    {
      cmd.erase(cmd.begin());
      continue;
    }
  }
}

int extent_server::tx_begin()
{
  chfs_command::cmd_type type = chfs_command::CMD_BEGIN;
  chfs_command command(type, 0, 0, "");
  _persister->append_log(command);
  return 0;
}

void extent_server::tx_commit(uint64_t id)
{
  chfs_command::cmd_type type = chfs_command::CMD_COMMIT;
  chfs_command command(type, id, 0, "");
  _persister->append_log(command);
}

int extent_server::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  printf("extent_server: create inode\n");
  id = im->alloc_inode(type);

  return extent_protocol::OK;
}

void extent_server::tx_create(uint64_t id, uint32_t type)
{
  chfs_command::cmd_type cmd_type = chfs_command::CMD_CREATE;
  chfs_command command(cmd_type, id, (uint64_t)type, "");
  _persister->append_log(command);
}
int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  id &= 0x7fffffff;

  const char *cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(id, cbuf, size);

  return extent_protocol::OK;
}

void extent_server::tx_put(uint64_t id, uint64_t exid, std::string str)
{
  chfs_command::cmd_type type = chfs_command::CMD_PUT;
  chfs_command command(type, id, exid, str);
  _persister->append_log(command);
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
  else
  {
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
  im->remove_file(id);

  return extent_protocol::OK;
}

void extent_server::tx_remove(uint64_t id, uint64_t exid)
{
  chfs_command::cmd_type type = chfs_command::CMD_REMOVE;
  chfs_command command(type, id, exid, "");
  _persister->append_log(command);
}