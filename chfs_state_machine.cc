#include "chfs_state_machine.h"
chfs_command_raft::chfs_command_raft()
{
    res = std::make_shared<result>();
}
// for CREATE
chfs_command_raft::chfs_command_raft(command_type _type, uint32_t file_type)
{
    // Lab3: Your code here
    cmd_tp = _type;
    type = file_type;
    res = std::make_shared<result>();
}

// for GET GETATTR REMOVE
chfs_command_raft::chfs_command_raft(command_type _type, extent_protocol::extentid_t _id)
{
    // Lab3: Your code here
    cmd_tp = _type;
    id = _id;
    res = std::make_shared<result>();
}

// for PUT
chfs_command_raft::chfs_command_raft(command_type _type, extent_protocol::extentid_t _id, std::string _buf)
{
    // Lab3: Your code here
    cmd_tp = _type;
    id = _id;
    buf = _buf;
    res = std::make_shared<result>();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) : cmd_tp(cmd.cmd_tp), type(cmd.type), id(cmd.id), buf(cmd.buf), res(cmd.res)
{
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft()
{
    // Lab3: Your code here
}

int chfs_command_raft::size() const
{
    // Lab3: Your code here
    int size = 0;
    switch (cmd_tp)
    {
    case CMD_CRT:
        size = 5;
        break;
    case CMD_PUT:
        size += buf.size();
    case CMD_GET:
    case CMD_GETA:
    case CMD_RMV:
        size += 9;
        break;
    case CMD_NONE:
    default:
        size = 0;
        break;
    }
    return size;
}

void chfs_command_raft::serialize(char *buf_out, int size) const
{
    // Lab3: Your code here
    switch (cmd_tp)
    {
    case CMD_CRT:
        buf_out[0] = cmd_tp & 0xff;
        transfer32ToBuf(buf_out + 1);
        break;
    case CMD_PUT:
    {
        int bufSize = buf.size();
        for (int i = 0; i < bufSize; i++)
        {
            buf_out[i + 9] = buf[i];
        }
    }
    case CMD_GET:
    case CMD_GETA:
    case CMD_RMV:
        buf_out[0] = cmd_tp & 0xff;
        transfer64ToBuf(buf_out + 1);
        break;
    case CMD_NONE:
    default:
        break;
    }
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size)
{
    // Lab3: Your code here
    switch (cmd_tp)
    {
    case CMD_CRT:
        setCmdType(buf_in[0] & 0xff);
        transferBufTo32(buf_in + 1);
        break;
    case CMD_PUT:
        buf = std::string(buf_in + 9, size - 9);
    case CMD_GET:
    case CMD_GETA:
    case CMD_RMV:
        setCmdType(buf_in[0] & 0xff);
        transferBufTo64(buf_in + 1);
        break;
    case CMD_NONE:
    default:
        break;
    }
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd)
{
    // Lab3: Your code here
    int cmd_tp_num = cmd.cmd_tp;
    m << cmd_tp_num << cmd.type << cmd.id << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd)
{
    // Lab3: Your code here
    int cmd_tp_num;
    u >> cmd_tp_num;
    cmd.setCmdType(cmd_tp_num);
    u >> cmd.type >> cmd.id >> cmd.buf;
    return u;
}

void chfs_command_raft::transfer64ToBuf(char *buf) const
{
    buf[0] = (id >> 56) & 0xff;
    buf[1] = (id >> 48) & 0xff;
    buf[2] = (id >> 40) & 0xff;
    buf[3] = (id >> 32) & 0xff;
    buf[4] = (id >> 24) & 0xff;
    buf[5] = (id >> 16) & 0xff;
    buf[6] = (id >> 8) & 0xff;
    buf[7] = id & 0xff;
}
void chfs_command_raft::transferBufTo64(const char *buf)
{
    id = (buf[0] & 0xff) << 56;
    id |= (buf[1] & 0xff) << 48;
    id |= (buf[2] & 0xff) << 40;
    id |= (buf[3] & 0xff) << 32;
    id |= (buf[4] & 0xff) << 24;
    id |= (buf[5] & 0xff) << 16;
    id |= (buf[6] & 0xff) << 8;
    id |= (buf[7] & 0xff);
}

void chfs_command_raft::transfer32ToBuf(char *buf) const
{
    buf[0] = (type >> 24) & 0xff;
    buf[1] = (type >> 16) & 0xff;
    buf[2] = (type >> 8) & 0xff;
    buf[3] = type & 0xff;
}
void chfs_command_raft::transferBufTo32(const char *buf)
{
    type = (buf[0] & 0xff) << 24;
    type |= (buf[1] & 0xff) << 16;
    type |= (buf[2] & 0xff) << 8;
    type |= buf[3] & 0xff;
}
void chfs_command_raft::setCmdType(int type)
{
    switch (type)
    {
    case 0:
        cmd_tp = CMD_NONE;
        break;
    case 1:
        cmd_tp = CMD_CRT;
        break;
    case 2:
        cmd_tp = CMD_PUT;
        break;
    case 3:
        cmd_tp = CMD_GET;
        break;
    case 4:
        cmd_tp = CMD_GETA;
        break;
    case 5:
        cmd_tp = CMD_RMV;
        break;
    default:
        break;
    }
}

void chfs_state_machine::apply_log(raft_command &cmd)
{
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(mtx);
    switch (chfs_cmd.cmd_tp)
    {
    case chfs_command_raft::command_type::CMD_CRT:
        es.create(chfs_cmd.type, chfs_cmd.res->id);
        printf("state_machine: create file, return id %d\n",chfs_cmd.res->id);
        break;
    case chfs_command_raft::command_type::CMD_GET:
        es.get(chfs_cmd.id, chfs_cmd.res->buf);
        printf("state_machine: get file %d, return %s\n",chfs_cmd.id,chfs_cmd.res->buf);
        break;
    case chfs_command_raft::command_type::CMD_GETA:
        es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
        printf("state_machine: getattr file %d\n",chfs_cmd.id);
        break;
    case chfs_command_raft::command_type::CMD_PUT:
    {
        int r;
        es.put(chfs_cmd.id, chfs_cmd.buf, r);
        printf("state_machine: put %s in file %d\n",chfs_cmd.buf,chfs_cmd.id);
        break;
    }
    case chfs_command_raft::command_type::CMD_RMV:
    {
        int r;
        es.remove(chfs_cmd.id, r);
        printf("state_machine: remove file %d\n",chfs_cmd.id);
        break;
    }
    default:
        break;
    }
    // The value of these fields should follow the definition in `chfs_state_machine.h` .
    chfs_cmd.res->done = true; // don't forget to set this
    chfs_cmd.res->cv.notify_all();
    return;
}
