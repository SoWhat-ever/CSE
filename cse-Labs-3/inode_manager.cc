#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  if (id < 0 || id >= BLOCK_NUM)
  {
    return;
  }
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM)
  {
    return;
  }
  memcpy(blocks[id], buf, BLOCK_SIZE);
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  for (int i = IBLOCK(INODE_NUM, sb.nblocks); i < BLOCK_NUM; i++)
  {
    if (!search_bitmap(i))
    {
      set_bitmap(i, true);
      printf("\talloc_block id:%d\n", i);
      return i;
    }
  }
  printf("\tim: alloc_block failed.\n");
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  if (id < 0 || id >= BLOCK_NUM)
  {
    return;
  }
  printf("\tfree_block %d\n", id);
  set_bitmap(id, false);
  return;
}

void block_manager::set_bitmap(uint32_t id, bool flag)
{
  char buf[BLOCK_SIZE];
  read_block(BBLOCK(id),buf);
  int index = id % BPB;
  if (flag)
  {
    buf[index / 8] |= (0x1 << (index % 8));
  }
  else
  {
    buf[index / 8] &= (~(0x1 << (index % 8)));
  }
  write_block(BBLOCK(id),buf);
}

bool block_manager::search_bitmap(uint32_t id)
{
  char buf[BLOCK_SIZE];
  read_block(BBLOCK(id),buf);
  int index = id % BPB;
  return buf[index / 8] & (0x1 << (index % 8));
}
// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  for (unsigned int i = 0; i < IBLOCK(INODE_NUM, sb.nblocks); i++)
    set_bitmap(i, true);
  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;
}

void block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1)
  {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /*
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  uint32_t inum = 1;
  printf("\tim: alloc_inode %d\n", type);
  while (get_inode(inum) != NULL)
  {
    inum++;
  }
  if (inum > INODE_NUM)
    return 0;
  struct inode *ino;
  ino = (struct inode *)malloc(sizeof(struct inode));
  memset(ino, 0, sizeof(struct inode));
  ino->type = type;
  ino->size = 0;
  unsigned int ttime = (unsigned int)time(NULL);
  ino->atime = ttime;
  ino->ctime = ttime;
  ino->mtime = ttime;
  put_inode(inum, ino);
  // free(ino);
  return inum;
}

void inode_manager::free_inode(uint32_t inum)
{
  /*
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode *ino = get_inode(inum);
  if (ino)
  {
    memset(ino, 0, sizeof(struct inode));
    ino->type = 0;
    put_inode(inum, ino);
    // free(ino);
  }
  else
  {
    printf("\tim: inode %d doesn't exist!\n", inum);
  }
  return;
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  char buf[BLOCK_SIZE];
  struct inode *ino, *ino_disk;

  printf("\tim: get_inode %d\n", inum);

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  if (ino_disk->type == 0)
    return NULL;

  ino = (struct inode *)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode *)buf + inum % IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

#define MIN(a, b) ((a) < (b) ? (a) : (b))

/* Get all the data of a file by inum.
 * Return alloced data, should be freed by caller. */
void inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  char buf[BLOCK_SIZE];
  int i = 0;
  struct inode *ino = get_inode(inum);
  if (ino != NULL)
  {
    unsigned int tmp_size = ino->size;
    if (tmp_size > MAXFILE * BLOCK_SIZE)
    {
      printf("\tim: size too large!\n");
      exit(0);
    }
    int whole_block_num = tmp_size / BLOCK_SIZE; //完整的block数量
    int remain_size = tmp_size % BLOCK_SIZE;
    *size = tmp_size;
    *buf_out = (char *)malloc(tmp_size);

    for (; i < whole_block_num; i++)
    {
      bm->read_block(get_blockid(ino, i), buf);
      memcpy(*buf_out + i * BLOCK_SIZE, buf, BLOCK_SIZE);
    }
    if (remain_size)
    {
      bm->read_block(get_blockid(ino, i), buf);
      memcpy(*buf_out + i * BLOCK_SIZE, buf, remain_size);
    }
  }
  else
  {
    printf("\tim: inode %d doesn't exist!\n", inum);
  }
  return;
}

blockid_t inode_manager::get_blockid(struct inode *ino, uint32_t n)
{
  char buf[BLOCK_SIZE];
  if (n < NDIRECT)
  {
    return ino->blocks[n];
  }
  else
  {
    bm->read_block((ino->blocks[NDIRECT]), buf);
    return ((blockid_t *)buf)[n - NDIRECT];
  }
  return 0;
}

void inode_manager::alloc_block(struct inode *ino, uint32_t n)
{
  char buf[BLOCK_SIZE];
  if (n < NDIRECT)
  {
    ino->blocks[n] = bm->alloc_block();
  }
  else
  {
    if (!ino->blocks[NDIRECT])
      ino->blocks[NDIRECT] = bm->alloc_block();
    bm->read_block((ino->blocks[NDIRECT]), buf);
    ((blockid_t *)buf)[n - NDIRECT] = bm->alloc_block();
    bm->write_block((ino->blocks[NDIRECT]), buf);
  }
  return;
}
/* alloc/free blocks if needed */
void inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf
   * is larger or smaller than the size of original inode
   */
  if (size < 0 || (unsigned int)size > MAXFILE * BLOCK_SIZE)
  {
    return;
  }
  char remain_file[BLOCK_SIZE];
  struct inode *ino = get_inode(inum);
  int whole_block_num = size / BLOCK_SIZE;
  int remain_size = size % BLOCK_SIZE;
  if (ino != NULL)
  {
    int inode_block_num, buf_block_num;
    // block_num向上取整
    if (ino->size == 0)
    {
      inode_block_num = 0;
    }
    else
    {
      inode_block_num = (ino->size - 1) / BLOCK_SIZE + 1;
    }
    if (size == 0)
    {
      buf_block_num = 0;
    }
    else
    {
      buf_block_num = (size - 1) / BLOCK_SIZE + 1;
    }

    if (inode_block_num > buf_block_num)
    {
      for (int i = buf_block_num; i < inode_block_num; i++)
      {
        bm->free_block(get_blockid(ino, i));
      }
    }
    else if (buf_block_num > inode_block_num)
    {
      for (int i = inode_block_num; i < buf_block_num; i++)
      {
        alloc_block(ino, i);
      }
    }

    int index = 0;
    for (; index < whole_block_num; index++)
    {
      bm->write_block(get_blockid(ino, index), buf + index * BLOCK_SIZE);
    }
    if (remain_size > 0)
    {
      memcpy(remain_file, buf + index * BLOCK_SIZE, remain_size);
      bm->write_block(get_blockid(ino, index), remain_file);
    }
    unsigned int ttime = (unsigned int)time(NULL);
    ino->size = size;
    ino->atime = ttime;
    ino->mtime = ttime;
    ino->ctime = ttime;
    put_inode(inum, ino);
    free(ino);
  }
  else
  {
    printf("\tim: inode %d doesn't exist!\n", inum);
  }
  return;
}

void inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  printf("\tim: get_attr %d\n", inum);
  struct inode *ino = get_inode(inum);

  if (ino == NULL)
  {
    a.type = 0;
    return;
  }
  a.type = ino->type;
  a.atime = ino->atime;
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  a.size = ino->size;
  return;
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode *ino = get_inode(inum);
  int block_num;
  if (ino->size == 0)
    block_num = 0;
  else
  {
    block_num = (ino->size - 1) / BLOCK_SIZE + 1;
  }
  for (int i = 0; i < block_num; i++)
    bm->free_block(get_blockid(ino, i));
  if (block_num > NDIRECT)
    bm->free_block(ino->blocks[NDIRECT]);

  free_inode(inum);
  return;
}
