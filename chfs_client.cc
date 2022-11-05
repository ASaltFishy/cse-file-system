// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"


chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
    lc->acquire(1);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
    lc->release(1);
}

// transfer the inum in string to unsigned long
chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        lc->release(inum);
        printf("error getting attr\n");
        return false;
    }

    lc->release(inum);
    if (a.type == extent_protocol::T_FILE)
    {
        printf("isfile: %lld is a file\n", inum);
        return true;
    }
    printf("isfile: %lld is a dir\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 *
 * */

bool chfs_client::isdir(inum inum)
{
    // Oops! is this still correct when you implement symlink?
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        lc->release(inum);
        printf("error getting attr\n");
        return false;
    }

    lc->release(inum);
    if (a.type == extent_protocol::T_DIR)
    {
        printf("isdir: %lld is a dir\n", inum);
        return true;
    }
    return false;
}

bool chfs_client::islink(inum inum){
    lc->acquire(inum);
    bool ret = !(isfile(inum)|isdir(inum));
    lc->release(inum);
    return ret;
}

int chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("getfile %016llx\n", inum);
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("getfile %016llx -> sz %llu\n", inum, fin.size);

release:
    lc->release(inum);
    return r;
}

int chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("getdir %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK)
    {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;

release:
    lc->release(inum);
    return r;
}

#define EXT_RPC(xx)                                                \
    do                                                             \
    {                                                              \
        if ((xx) != extent_protocol::OK)                           \
        {                                                          \
            printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
            r = IOERR;                                             \
            goto release;                                          \
        }                                                          \
    } while (0)

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    lc->acquire(ino);
    std::string buf;
    r = ec->get(ino, buf);
    if (r != xxstatus::OK)
        return r;

    buf.resize(size);
    r = ec->put(ino, buf);
    lc->release(ino);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    lc->acquire(parent);
    r = lookup(parent, name, found, ino_out);
    if (r == IOERR)
    {
        printf("chfs_client::create: no such parent dir,make now\n");
        lc->release(parent);
        return r;
    }
    if (found){
        lc->release(parent);
        return EXIST;
    }
    
    ec->beginTX(ino_out);
    // all the create need to aquire the lock 0
    lc->acquire(0);
    r = ec->create(extent_protocol::T_FILE, ino_out);
    lc->acquire(ino_out);
    std::string buf;
    r = ec->get(parent, buf);
    buf.append(std::string(name) + '/' + filename(ino_out) + '/');
    r = ec->put(parent, buf);
    ec->commitTX(ino_out);
    lc->release(ino_out);
    lc->release(0);
    lc->release(parent);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;

    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    bool found = false;
    lc->acquire(parent);
    r = lookup(parent, name, found, ino_out);
    if (r == IOERR)
    {
        printf("chfs_client::create: no such parent dir,make now\n");
        lc->release(parent);
        return r;
    }
    if (found){
        lc->release(parent);
        return EXIST;
    }
        

    ec->beginTX(ino_out);
    lc->acquire(0);
    r = ec->create(extent_protocol::T_DIR, ino_out);
    lc->acquire(ino_out);
    std::string buf;
    r = ec->get(parent, buf);
    buf.append(std::string(name) + '/' + filename(ino_out) + '/');
    r = ec->put(parent, buf);
    ec->commitTX(ino_out);
    lc->release(ino_out);
    lc->release(0);
    lc->release(parent);
    return r;
}

int chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    found = false;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    std::list<dirent> list;
    lc->acquire(parent);
    r = readdir(parent, list);
    lc->release(parent);
    if (r != OK)
    {
        return r;
    }

    for (std::list<dirent>::iterator it = list.begin(); it != list.end(); ++it)
    {
        if (it->name.compare(name) == 0)
        {
            found = true;
            ino_out = it->inum;
            return OK;
        }
    }
    return NOENT;
}

int chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */

    // my defined format: name/inum/name/inum/...
    lc->acquire(dir);
    if (!isdir(dir))
    {
        printf("chfs_client::readdir: dir:%lld do not exist", dir);
        return IOERR;
    }

    std::string buf;
    ec->get(dir, buf);
    lc->release(dir);
    struct dirent temp;
    unsigned long name_start = 0, name_end = buf.find('/');
    while (name_end != std::string::npos)
    {
        std::string name = buf.substr(name_start, name_end - name_start);
        int inum_start = name_end + 1;
        int inum_end = buf.find('/', inum_start);
        std::string inum = buf.substr(inum_start, inum_end);
        name_start = inum_end + 1;
        name_end = buf.find('/', name_start);
        temp.inum = n2i(inum);
        temp.name = name;
        list.push_back(temp);
    }
    return r;
}

int chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    printf("chfs_client::read file:%lld off:%ld size:%ld\n", ino,off,size);
    /*
     * your code goes here.
     * note: read using ec->get().
     */
    std::string buf;
    lc->acquire(ino);
    r = ec->get(ino, buf);
    lc->release(ino);
    if (r != OK)
    {
        printf("chfs_client::read: invalid inum %lld\n", ino);
        return r;
    }
    if (off >= buf.size())
    {
        data = "";
        return r;
    }
    // data = buf.substr(off, size);
    // return r;
    if (off + size > buf.size()) {
        data = buf.substr(off);
        return r;
    }

    // normal
    data = buf.substr(off, size);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    printf("chfs_client::write file:%lld off:%ld size:%ld", ino,off,size);
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    std::string buf;
    lc->acquire(ino);
    r = ec->get(ino, buf);
    if (r != OK)
    {
        printf("chfs_client::write: invalid inum %lld\n", ino);
        return r;
    }
    // fill the holes with '\0'
    if (off + size > buf.size())
    {
        buf.resize(off + size);
    }
    printf("\tbuf resize:%ld\n",buf.size());
    // buf = buf.replace(off, size, data);
    //用replace函数buf size会出现bug
    for(int i=off;i<off+size;i++){
        buf[i] = data[i-off];
    }
    printf("\tbuf size after replace:%ld\n",buf.size());
    bytes_written = size;
    ec->beginTX(ino);
    r = ec->put(ino,buf);
    ec->commitTX(ino);
    lc->release(ino);
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    bool found = false;
    inum ino;
    std::list<dirent> list;
    std::string buf;
    lc->acquire(parent);
    r = lookup(parent,name,found,ino);

    if(r!=OK){
        lc->release(parent);
        return r;
    }
    if(!found){
        lc->release(parent);
        return NOENT;
    }
    if(isdir(ino)){
        lc->release(parent);
        return IOERR;
    }

    lc->acquire(ino);
    ec->beginTX(ino);
    ec->remove(ino);
    ec->get(parent,buf);
    int start = buf.find(name);
    int middle = buf.find('/',start+1);
    int end = buf.find('/',middle+1);
    std::string temp = buf.substr(start, end-start);
    printf("unlink: content:%s\n",temp.data());
    buf.erase(start,end-start+1);
    ec->put(parent,buf);
    ec->commitTX(ino);
    lc->release(ino);
    lc->release(parent);
    return r;
}

int chfs_client::symlink(inum parent, const char *name, inum &ino_out,const char *content)
{
    int r = OK;
    bool found = false;
    lc->acquire(parent);
    r = lookup(parent, name, found, ino_out);
    if (r == IOERR)
    {
        printf("chfs_client::symlink: no such parent dir,make now\n");
        return r;
    }
    if (found)
        return EXIST;
    
    lc->acquire(0);
    ec->beginTX(ino_out);
    printf("chfs_client::symlink create symlink:%s content:%s ",name,content);
    r = ec->create(extent_protocol::T_SYMLINK,ino_out);
    printf("inum:%lld\n",ino_out);
    lc->acquire(ino_out);
    r = ec->put(ino_out,std::string(content));

    std::string buf;
    r = ec->get(parent,buf);
    buf.append(std::string(name) + '/' + filename(ino_out) + '/');
    r = ec->put(parent, buf);
    ec->commitTX(ino_out);
    lc->release(ino_out);
    lc->release(0);
    lc->release(parent);
    return r;
    
}

int chfs_client::readlink(inum ino,std::string &data)
{
    int r = OK;
    lc->acquire(ino);
    r = ec->get(ino, data);
    lc->release(ino);
    if (r != OK)
    {
        printf("chfs_client::readlink: invalid inum %lld\n", ino);
        return r;
    }
    printf("chfs_client::readlink:%lld, content:%s\n",ino,data.data());
    return r;
}