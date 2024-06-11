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
    enum cmd_type
    {
        CMD_BEGIN = 0,
        CMD_COMMIT,
        CMD_CREATE,
        CMD_PUT,
        CMD_GET,
        CMD_GETATTR,
        CMD_REMOVE

    };

    cmd_type type = CMD_BEGIN;
    txid_t id = 0;
    std::string content = "";
    uint64_t exid = 0;

    // constructor
    chfs_command() {}
    chfs_command(cmd_type _type, txid_t _id, uint64_t _exid, std::string _content)
    {
        type = _type;
        id = _id;
        exid = _exid;
        content = _content;
    }
    uint64_t size() const
    {
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t);
        return s;
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
    std::vector<command> restore_logdata();
    void restore_checkpoint();

private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;

    // restored log data
    std::vector<command> log_entries;
};

template <typename command>
persister<command>::persister(const std::string &dir)
{
    // DO NOT change the file names here
    file_dir = dir;
    file_path_checkpoint = file_dir + "/logdata.bin";
    file_path_logfile = file_dir + "/checkpoint.bin";
}

template <typename command>
persister<command>::~persister()
{
    // Your code here for lab2A
    log_entries.clear();
    std::vector<command>().swap(log_entries);
}

template <typename command>
void persister<command>::append_log(const command &log)
{
    // Your code here for lab2A
    uint32_t type = log.type;
    uint64_t id = log.id;
    uint64_t exid = log.exid;
    std::string str = log.content;
    uint64_t content_size = str.size();
    std::ofstream outFile;
    outFile.open(file_path_logfile, std::ios::app | std::ios::binary);
    log_entries.push_back(log);
    outFile.write((char *)&type, sizeof(type));
    outFile.write((char *)&id, sizeof(id));
    outFile.write((char *)&exid, sizeof(exid));
    outFile.write((char *)&content_size, sizeof(content_size));
    outFile.write(str.c_str(), content_size);
    outFile.close();
}

template <typename command>
void persister<command>::checkpoint()
{
    // Your code here for lab2A
}

template <typename command>
std::vector<command> persister<command>::restore_logdata()
{
    // Your code here for lab2A
    log_entries.clear();
    std::vector<command>().swap(log_entries);
    std::ifstream inFile;
    inFile.open(file_path_logfile, std::ios::binary);
    while (inFile.peek() != EOF)
    {
        chfs_command::cmd_type type;
        chfs_command::txid_t id;
        uint64_t exid;
        uint64_t content_size;
        char ch;
        inFile.read((char *)&type, sizeof(type));
        inFile.read((char *)&id, sizeof(id));
        inFile.read((char *)&exid, sizeof(exid));
        inFile.read((char *)&content_size, sizeof(content_size));
        std::string str;
        for (uint64_t i = 0; i < content_size; i++)
        {
            inFile.read((char *)&ch, sizeof(ch));
            str += ch;
        }
        command log(type, id, exid, str);
        log_entries.push_back(log);
    }
    inFile.close();
    return log_entries;
};

template <typename command>
void persister<command>::restore_checkpoint(){
    // Your code here for lab2A

};

using chfs_persister = persister<chfs_command>;

#endif // persister_h