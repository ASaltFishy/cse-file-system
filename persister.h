#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define MAX_LOG_SZ 131072

/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires.
 *
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */

class chfs_command
{
public:
    typedef unsigned long long txid_t;
    typedef unsigned long size_t;
    typedef unsigned int inode_num_t;
    enum cmd_type
    {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_REMOVE
    };
    // record the newest id
    static txid_t latest_id;
    // attempt to design a value log
    cmd_type type = CMD_BEGIN;
    txid_t id;
    size_t entrysize = 0;
    size_t oldsize, newsize;
    inode_num_t inum;
    std::string oldbuf, newbuf;
    uint32_t fileType;

    // constructor for BEGIN and COMMIT
    // only called when writing log
    chfs_command(cmd_type _type)
    {
        // when reach a commit point, latest id ++
        type = _type;
        id = _type == CMD_COMMIT ? ++latest_id : latest_id;
        entrysize = getSize();
    }
    // constructor for CREATE
    chfs_command(cmd_type _type, inode_num_t alloc_id, uint32_t _fileType, txid_t _id = 0, bool isRead = false)
    {
        type = _type;
        id = isRead ? _id : latest_id;
        inum = alloc_id;
        fileType = _fileType;
        entrysize = getSize();
    }
    // constructor for PUT
    chfs_command(cmd_type _type, inode_num_t alloc_id, size_t _oldsize, std::string _oldbuf, size_t _newsize, std::string _newbuf, txid_t _id = 0, bool isRead = false)
    {
        type = _type;
        id = isRead ? _id : latest_id;
        inum = alloc_id;
        oldsize = _oldsize;
        newsize = _newsize;
        oldbuf = _oldbuf;
        newbuf = _newbuf;
        entrysize = getSize();
    }
    // constructor for REMOVE
    chfs_command(cmd_type _type, inode_num_t alloc_id, uint32_t _fileType, size_t _oldsize, std::string _oldbuf, txid_t _id = 0, bool isRead = false)
    {
        type = _type;
        id = isRead ? _id : latest_id;
        inum = alloc_id;
        fileType = _fileType;
        oldsize = _oldsize;
        oldbuf = _oldbuf;
        entrysize = getSize();
    }

    // size of the header of each entry
    uint64_t getSize() const
    {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t) + sizeof(size_t);
        switch (type)
        {
        case CMD_BEGIN:
        case CMD_COMMIT:
            break;
        case CMD_CREATE:
            s += sizeof(inode_num_t) + sizeof(uint32_t);
            break;
        case CMD_REMOVE:
            s += sizeof(inode_num_t) + sizeof(uint32_t) + oldbuf.size() + sizeof(size_t);
            break;
        case CMD_PUT:
            s += sizeof(inode_num_t) + oldbuf.size() + 2 * sizeof(size_t) + newbuf.size();
            break;
        default:
            break;
        }
        return s;
    }

    void toEntry(std::ofstream *output) const
    {
        switch (type)
        {
        case CMD_BEGIN:
        case CMD_COMMIT:
            printf("toEntry: save begin or commit now!\t");
            output->write((char *)&id, sizeof(txid_t));
            output->write((char *)&type, sizeof(cmd_type));
            output->write((char *)&entrysize, sizeof(size_t));
            break;
        case CMD_CREATE:
            printf("toEntry: save create now!\t");
            output->write((char *)&id, sizeof(id));
            output->write((char *)&type, sizeof(type));
            output->write((char *)&inum, sizeof(inum));
            output->write((char *)&fileType, sizeof(fileType));
            output->write((char *)&entrysize, sizeof(entrysize));
            break;
        case CMD_REMOVE:
            printf("toEntry: save remove now!\t");
            output->write((char *)&id, sizeof(txid_t));
            output->write((char *)&type, sizeof(cmd_type));
            output->write((char *)&inum, sizeof(inode_num_t));
            output->write((char *)&fileType, sizeof(fileType));
            output->write((char *)&oldsize, sizeof(size_t));
            output->write(oldbuf.data(), oldsize);
            output->write((char *)&entrysize, sizeof(size_t));
            break;
        case CMD_PUT:
            printf("toEntry: save put now!\t");
            output->write((char *)&id, sizeof(txid_t));
            output->write((char *)&type, sizeof(cmd_type));
            output->write((char *)&inum, sizeof(inode_num_t));
            output->write((char *)&oldsize, sizeof(size_t));
            output->write(oldbuf.data(), oldsize);
            output->write((char *)&newsize, sizeof(size_t));
            output->write(newbuf.data(), newsize);
            output->write((char *)&entrysize, sizeof(size_t));
            break;
        default:
            break;
        }
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to
 * persist and recover data.
 *
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template <typename command>
class persister
{

public:
    persister(const std::string &file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(const command &log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    std::vector<command> &restore_logdata();
    void restore_checkpoint();

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;
    int logSize = 0;

    // restored log data
    std::vector<command> log_entries;
    void parseEntry(std::ifstream *input);
};

template <typename command>
persister<command>::persister(const std::string &dir)
{
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
}

template <typename command>
persister<command>::~persister()
{
    // Your code here for lab2A
    // it seems nothing to do.....
}

template <typename command>
void persister<command>::append_log(const command &log)
{
    // Your code here for lab2A
    std::ofstream *outFile = new std::ofstream();
    outFile->open(file_path_logfile, std::ofstream::binary | std::ios::app);
    log.toEntry(outFile);
    outFile->close();
    delete outFile;
}

template <typename command>
void persister<command>::checkpoint()
{
    // Your code here for lab2A
}

template <typename command>
std::vector<command> &persister<command>::restore_logdata()
{
    // Your code here for lab2A
    // no transaction now and just redo from start to end
    std::ifstream *inFile = new std::ifstream();
    inFile->open(file_path_logfile, std::ofstream::binary);
    if (inFile->is_open())
    {
        printf("persist: open log file successfuly!\n");
        parseEntry(inFile);
    }
    else
    {
        printf("persist: log file not exist, skip parsing!\n");
    }
    inFile->close();
    delete inFile;
    return log_entries;
};

template <typename command>
void persister<command>::parseEntry(std::ifstream *input)
{
    printf("parse: start function now!\n");
    chfs_command::txid_t id;
    chfs_command::size_t entrySize, oldsize, newsize;
    chfs_command::cmd_type type;
    chfs_command::inode_num_t inum;
    std::string newbuf, oldbuf;
    uint32_t fileType;
    while (true)
    {
        input->read((char *)&id, sizeof(id));
        printf("parse: read txid: %lld\t", id);
        input->read((char *)&type, sizeof(type));
        printf("type: %d\t", type);
        if (input->eof())
            break;

        switch (type)
        {
        case chfs_command::CMD_BEGIN:
        case chfs_command::CMD_COMMIT:
        {
            input->read((char *)&entrySize, sizeof(chfs_command::size_t));
            chfs_command::latest_id = id;
            break;
        }
        case chfs_command::CMD_CREATE:
        {
            input->read((char *)&inum, sizeof(chfs_command::inode_num_t));
            printf("inum: %d\n", inum);
            input->read((char *)&fileType, sizeof(fileType));
            printf("filetype: %d\n", fileType);
            input->read((char *)&entrySize, sizeof(chfs_command::size_t));
            chfs_command acmd(type, inum, fileType, id, true);
            log_entries.push_back(acmd);
            break;
        }
        case chfs_command::CMD_PUT:
        {
            input->read((char *)&inum, sizeof(chfs_command::inode_num_t));
            printf("inum: %d\t", inum);

            input->read((char *)&oldsize, sizeof(chfs_command::size_t));
            printf("oldsize: %ld\t", oldsize);

            if (oldsize != 0)
            {
                char buf[oldsize];
                input->read(buf, oldsize);
                oldbuf.clear();
                oldbuf.assign(buf, oldsize);
                printf("exactly:%ld buf:%s\t", input->gcount(), oldbuf.data());
            }

            input->read((char *)&newsize, sizeof(chfs_command::size_t));
            printf("newsize: %ld\t", newsize);

            char buf[newsize];
            input->read(buf, newsize);
            newbuf.clear();
            newbuf.assign(buf, newsize);
            printf("exactly:%ld\t buf:%s", input->gcount(), newbuf.data());

            input->read((char *)&entrySize, sizeof(chfs_command::size_t));
            printf("entrysize: %ld\n", entrySize);
            chfs_command acmd(type, inum, oldsize, oldbuf, newsize, newbuf, id, true);
            log_entries.push_back(acmd);
            break;
        }
        case chfs_command::CMD_REMOVE:
        {
            input->read((char *)&inum, sizeof(chfs_command::inode_num_t));
            printf("inum: %d\t", inum);

            input->read((char *)&fileType, sizeof(fileType));
            printf("filetype: %d\n", fileType);

            input->read((char *)&oldsize, sizeof(chfs_command::size_t));
            printf("oldsize: %ld\t", oldsize);

            if (oldsize != 0)
            {
                char buf[oldsize];
                input->read(buf, oldsize);
                oldbuf.clear();
                oldbuf.assign(buf, oldsize);
                printf("exactly:%ld buf:%s\t", input->gcount(), oldbuf.data());
            }

            input->read((char *)&entrySize, sizeof(chfs_command::size_t));
            printf("entrysize: %ld\n", entrySize);
            chfs_command acmd(type, inum, fileType, oldsize, oldbuf, id, true);
            log_entries.push_back(acmd);
            break;
        }
        default:
            break;
        }
    }
    // undo transaction
    int back = log_entries.size() - 1;
    for (int i = back; i >= 0; i--)
    {
        chfs_command temp = log_entries[i];
        if (temp.id != chfs_command::latest_id)
            break;
        log_entries.pop_back();
    }
}

template <typename command>
void persister<command>::restore_checkpoint(){
    // Your code here for lab2A
};

using chfs_persister = persister<chfs_command>;
using chfs_command = chfs_command;

#endif // persister_h