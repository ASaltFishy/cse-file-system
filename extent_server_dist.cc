#include "extent_server_dist.h"

chfs_raft *extent_server_dist::leader() const
{
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0)
    {
        return this->raft_group->nodes[0];
    }
    else
    {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id)
{
    // Lab3: your code here
    printf("----CREATE: type-%d\n",type);
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_CRT, type);
    int term, index;

    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    cmd.res->start = std::chrono::system_clock::now();
    leader()->new_command(cmd, term, index);
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
        printf("CREATE cmd done\n");
    }
    id = cmd.res->id;
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    // Lab3: your code here
    printf("----PUT: id-%d \n", id);
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_PUT, id, buf);
    int term, index;

    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    printf("here!\n");
    cmd.res->start = std::chrono::system_clock::now();
    printf("before new command\n");

    leader()->new_command(cmd, term, index);
    // printf("after new command\n");
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
        printf("PUT cmd done\n");
    }
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf)
{
    printf("----GET: id-%d\n",id);
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_GET, id);
    int term, index;

    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    cmd.res->start = std::chrono::system_clock::now();
    leader()->new_command(cmd, term, index);
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
        printf("GET cmd done\n");
    }
    buf = cmd.res->buf;
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    printf("----GETATTR: id-%d\n",id);
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_GETA, id);
    int term, index;

    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    printf("herea\n");
    cmd.res->start = std::chrono::system_clock::now();
    printf("before new command\n");
    leader()->new_command(cmd, term, index);
    printf("after new command\n");
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
        printf("GETATTR cmd done\n");
    }
    a = cmd.res->attr;
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &)
{
    printf("----REMMOVE: id-%d\n",id);
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_RMV, id);
    int term, index;

    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    cmd.res->start = std::chrono::system_clock::now();
    leader()->new_command(cmd, term, index);
    if (!cmd.res->done)
    {
        ASSERT(
            cmd.res->cv.wait_until(lock, cmd.res->start + std::chrono::seconds(3)) == std::cv_status::no_timeout,
            "extent_server_dist::create command timeout");
        printf("REMOVE cmd done\n");
    }
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist()
{
    delete this->raft_group;
}