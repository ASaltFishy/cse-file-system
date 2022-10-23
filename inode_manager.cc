#include "inode_manager.h"

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void disk::read_block(blockid_t id, char *buf)
{
  // corner condition--considering the validatio of id and buf
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;
  memcpy(buf, blocks[id], BLOCK_SIZE);
}

void disk::write_block(blockid_t id, const char *buf)
{
  if (id < 0 || id >= BLOCK_NUM || buf == NULL)
    return;
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
  //遍历从左到右bitmap寻找free block
  blockid_t begin_id = IBLOCK(sb.ninodes, sb.nblocks) + 1;
  // printf("\tbegin_id:%d",begin_id);
  char *buf = (char*)malloc(BLOCK_SIZE);

  //根据blockid逐个查询bitmap的有效情况，直到找到空位为止
  for(int i=begin_id;i<BLOCK_NUM;i++){
    blockid_t block_num = BBLOCK(i);
    blockid_t offset = (i%BPB)/8;
    blockid_t bit = i%8;
    d->read_block(block_num, buf);
    char temp = *(buf+offset);
    if(temp!=0xff){
      // printf("\tchar: %x pointer: %p",temp,buf);
      if (((temp >> (7 - bit)) & 1) == 0){
        // printf("\talloc block now! bitmap: %d,offset: %d, bit: %d\n",block_num,offset,bit);
        *(buf+offset) = temp | (1 << (7 - bit));
        // printf("\t after write:%x pointer:%p",*(buf+offset),buf);
        write_block(block_num, buf);
        free(buf);
        return i;
      }
    }
  }
  free(buf);
  return 0;
}

void block_manager::free_block(uint32_t id)
{
  /*
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  if (id < 0 || id >= BLOCK_NUM)
    return;

  blockid_t bitmap = BBLOCK(id);
  int block_location = (id % BPB) / 8;
  int bit_location = (id % BPB) % 8;
  char buf[BLOCK_SIZE];
  d->read_block(bitmap, buf);
  char temp = buf[block_location];
  buf[block_location] = temp & ~(0x1 << (7 - bit_location)) ;
  d->write_block(bitmap, buf);
  // free(buf);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

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
  uint32_t inodeNum;
  struct inode *temp;
  for (inodeNum = 1; inodeNum <= bm->sb.ninodes; inodeNum++)
  {
    temp = get_inode(inodeNum);
    if (temp == NULL )
      break;
    free(temp);
  }

  temp = (struct inode *)malloc(sizeof(struct inode));
  bzero(temp,sizeof(struct inode));
  int t = time(0);
  temp->atime = t;
  temp->ctime = t;
  temp->mtime = t;
  temp->size = 0;
  temp->type = type;

  put_inode(inodeNum, temp);
  free(temp);
  if (inodeNum <= bm->sb.ninodes)
    return inodeNum;
  else{
    printf("error: alloc_inode failed!\n");
    return 0;
  }
    
}

void inode_manager::free_inode(uint32_t inum)
{
  /*
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode *ino = get_inode(inum);
  // already be free
  if (ino == NULL)
    return;

  int size = ino->size;
  int block_num = (size+BLOCK_SIZE-1)/BLOCK_SIZE;
  if(block_num<=NDIRECT){
    for(int i=0;i<block_num;i++){
      bm->free_block(ino->blocks[i]);
    }
  }else{
    for(int i=0;i<NDIRECT;i++){
      bm->free_block(ino->blocks[i]);
    }
    char idirect_id[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], idirect_id);
    for(int i=NDIRECT;i<block_num;i++){
      blockid_t id = *((blockid_t *)idirect_id + i-NDIRECT);
      bm->free_block(id);
    }
    bm->free_block(ino->blocks[NDIRECT]);
  }
  ino->size = 0;
  ino->type = 0;
  put_inode(inum,ino);
  free(ino);
}

/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode *
inode_manager::get_inode(uint32_t inum)
{
  if (inum < 0 || inum > bm->sb.ninodes)
    return NULL;
  struct inode *ino,*ret;
  char buf[BLOCK_SIZE];
  // printf("\tdebug:getinode%d",inum);
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino = ((struct inode *)buf + (inum % IPB));

  if (ino->type == 0)
  {
    printf("\tim: inode not exist\n");
    return NULL;
  }
  ret = (struct inode *)malloc(sizeof(struct inode));
  // memcpy(ret, ino, sizeof(struct inode));
  *ret = *ino;
  // printf("\tret:%p ret type:%d\n",ret,ret->type);
  return ret;
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
  printf("\tread file%d",inum);
  struct inode *ino = get_inode(inum);
  //inode未被分配，直接返回
  if (ino == NULL)
  {
    *size = 0;
    return;
  }
  *size = ino->size;
  //文件大小为0，修改atime后直接返回
  if(*size==0){
    ino->atime = time(NULL);
    put_inode(inum,ino);
    free(ino);
    return;
  }
  printf("\tdebug: size %d",*size);
  int block_num = (*size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  char *buf = (char *)malloc(block_num * BLOCK_SIZE);
  printf("\tblock_num %d\n",block_num);
  //只用到了直接块
  if (block_num <= NDIRECT)
  {
    for (int i = 0; i < block_num-1; i++)
    {
      bm->read_block(ino->blocks[i], buf + i * BLOCK_SIZE);
    }
    //最后一块单独读取，不读多余内容
    char last_block[BLOCK_SIZE];
    bm->read_block(ino->blocks[block_num-1],last_block);
    int last_size = ino->size-(block_num-1)*BLOCK_SIZE;
    memcpy(buf+(block_num-1)*BLOCK_SIZE,last_block,last_size);
  }
  //文件较长用到了间接块
  else
  {
    for (int i = 0; i < NDIRECT; i++)
    {
      bm->read_block(ino->blocks[i], buf + i * BLOCK_SIZE);
    }
    //获取间接块id
    char idirect_id[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], idirect_id);
    for (int i = NDIRECT; i < block_num - 1; i++)
    {
      blockid_t id = *((blockid_t *)idirect_id + i-NDIRECT);
      bm->read_block(id, buf + i * BLOCK_SIZE);
    }
    //最后一块单独读取，不读多余内容
    char last_block[BLOCK_SIZE];
    blockid_t last_id = *((blockid_t *)idirect_id + block_num -1 -NDIRECT);
    bm->read_block(last_id,last_block);
    int last_size = ino->size-(block_num-1)*BLOCK_SIZE;
    memcpy(buf+(block_num-1)*BLOCK_SIZE,last_block,last_size);
  }
  free(ino);
  *buf_out = buf;
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
  printf("\tdebug:write file:%d size:%d",inum,size);
  struct inode *ino = get_inode(inum);
  // printf("\tdebug:ino:%p",ino);
  // printf("\tsize:",ino->size);
  // printf("\ttype:",ino->type);
  int old_block_num = (ino->size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  int new_block_num = (size + BLOCK_SIZE - 1) / BLOCK_SIZE;
  printf("\told_block_size:%d\tnew_block_size:%d\n",old_block_num,new_block_num);

  //释放原有的多余blocks
  if (old_block_num >= new_block_num)
  {
    if (old_block_num <= NDIRECT)
    {
      for (int i = new_block_num; i < old_block_num; i++)
        bm->free_block(ino->blocks[i]);
    }
    else if (new_block_num > NDIRECT)
    {
      char idirect_id[BLOCK_SIZE];
      bm->read_block(ino->blocks[NDIRECT], idirect_id);
      for(int i=new_block_num;i<old_block_num;i++){
        blockid_t id = *((blockid_t *)idirect_id + i-NDIRECT);
        bm->free_block(id);
      }
    }
    else
    {
      char idirect_id[BLOCK_SIZE];
      bm->read_block(ino->blocks[NDIRECT], idirect_id);
      for(int i=NDIRECT;i<old_block_num;i++){
        blockid_t id = *((blockid_t *)idirect_id + i-NDIRECT);
        bm->free_block(id);
      }
      bm->free_block(ino->blocks[NDIRECT]);
      for(int i=new_block_num;i<NDIRECT;i++){
        bm->free_block(ino->blocks[i]);
      }
    }
  }
  //添加数目不足的blocks
  else
  {
    if(old_block_num>NDIRECT){
      char idirect_id[BLOCK_SIZE];
      //已有间接块，无需分配
      bm->read_block(ino->blocks[NDIRECT], idirect_id);
      for(int i=old_block_num;i<new_block_num;i++){
        *((blockid_t *)idirect_id + i-NDIRECT) = bm->alloc_block();
      }
      bm->write_block(ino->blocks[NDIRECT],idirect_id);
    }
    else if(new_block_num<=NDIRECT){
      for(int i=old_block_num;i<new_block_num;i++){
        ino->blocks[i] = bm->alloc_block();
      }
      printf("\tbegin:%d\tend:%d\n",ino->blocks[0],ino->blocks[new_block_num-1]);
    }
    else{
      for(int i=old_block_num;i<NDIRECT;i++){
        ino->blocks[i] = bm->alloc_block();
      }
      printf("\tbegin:%d",ino->blocks[0]);
      char idirect_id[BLOCK_SIZE];
      //分配一个间接块存新一批block_num
      blockid_t new_block = bm->alloc_block();
      ino->blocks[NDIRECT] = new_block;
      for(int i=NDIRECT;i<new_block_num;i++){
        *((blockid_t *)idirect_id + i-NDIRECT) = bm->alloc_block();
      }
      printf("\tend:%d\n",*((blockid_t *)idirect_id + new_block_num-1-NDIRECT));
      bm->write_block(new_block,idirect_id);
    }

  }

  //写入新blocks
  if (new_block_num <= NDIRECT)
  {
    for (int i = 0; i < new_block_num; i++)
      bm->write_block(ino->blocks[i], buf + i * BLOCK_SIZE);
  }
  else
  {
    for(int i=0;i<NDIRECT;i++){
      bm->write_block(ino->blocks[i],buf+i*BLOCK_SIZE);
    }
    char idirect_id[BLOCK_SIZE];
    bm->read_block(ino->blocks[NDIRECT], idirect_id);
    for(int i=NDIRECT;i<new_block_num;i++){
      blockid_t id = *((blockid_t *)idirect_id + i-NDIRECT);
      bm->write_block(id,buf+i*BLOCK_SIZE);
    }
  }
  //将新的inode数据写回
  ino->size = size;
  ino->atime = (unsigned int)time(NULL);
  ino->mtime = (unsigned int)time(NULL);
  ino->ctime = (unsigned int)time(NULL);
  put_inode(inum,ino);
  free(ino);
  return;
}

void inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  printf("\tdebug:get attr %d\n",inum);
  struct inode *ino = get_inode(inum);
  if (ino == NULL){
    printf("\tdebug:inode==NULL when get attr\n");
    return;
  }

  a.atime = ino->atime;
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  a.size = ino->size;
  a.type = ino->type;

  free(ino);
}

void inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  free_inode(inum);
  return;
}
