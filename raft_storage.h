#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>
#include <fstream>

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    int current_term;
    int votedFor;
    std::vector<log_entry<command>> logs;

    void flush();
    void restore();

private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string file_path;
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    // restore after recovery
    file_path = dir + "/raftlog";
    current_term = 0;
    votedFor = -1;
    log_entry<command> empty_entry;
    empty_entry.index = 0;
    empty_entry.term = 0;
    logs.push_back(empty_entry);
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
    // flush to disk
    flush();
}

template <typename command>
void raft_storage<command>::flush() {

    mtx.lock();
    std::ofstream outFile(file_path,std::ofstream::binary);
     if (outFile.is_open()){
        outFile.write((char *)&current_term,sizeof(int));
        outFile.write((char *)&votedFor,sizeof(int));
        int size = logs.size();
        // printf("flush: log size:%d\n",size);
        outFile.write((char *)&size,sizeof(int));
        for(log_entry<command> item : logs){
            int cmdSize = item.cmd.size();
            char *buf = new char[cmdSize];
            item.cmd.serialize(buf,cmdSize);

            outFile.write((char *)&item.index,sizeof(int));
            outFile.write((char *)&item.term,sizeof(int));
            outFile.write((char *)&cmdSize,sizeof(int));
            outFile.write(buf,cmdSize);
            // printf("flush: write cmd index: %d term: %d cmdSize: %d\n",item.index, item.term, cmdSize);

            delete[] buf;
        }
     }
     outFile.close();
     mtx.unlock();
}

template <typename command>
void raft_storage<command>::restore() {
    mtx.lock();
    std::ifstream logFile(file_path,std::ofstream::binary);
    if(logFile.is_open()){
        logFile.read((char *)&current_term,sizeof(int));
        logFile.read((char *)&votedFor,sizeof(int));
        int size;
        logFile.read((char *)&size,sizeof(int));
        // printf("restore: log size:%d\n",size);
        
        logs.clear();
        for(int i=0;i<size;i++){
            log_entry<command> temp;
            int cmdSize;

            logFile.read((char *)&temp.index,sizeof(int));
            logFile.read((char *)&temp.term,sizeof(int));
            logFile.read((char *)&cmdSize,sizeof(int));
            // printf("restore: read cmd index: %d term: %d, cmdSize: %d\n",temp.index, temp.term ,cmdSize);
            char *buf = new char[cmdSize];
            logFile.read(buf,cmdSize);

            temp.cmd.deserialize(buf,cmdSize);
            logs.push_back(temp);

            delete[] buf;
        }
    }else{
        // printf("logfile is empty, no need to restore\n");
    }
    logFile.close();
    mtx.unlock();
}
#endif // raft_storage_h