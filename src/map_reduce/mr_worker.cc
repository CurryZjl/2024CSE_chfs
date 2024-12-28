#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <regex.h>

#include <mutex>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <unordered_map>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    std::string Worker::getContentFromFile(std::string filename){
        auto res_lookup = chfs_client->lookup(1, filename);
        if(res_lookup.is_err()){
            std::cerr<<"ERROR IN GETCONT(LOOKUP) WITH FILE "<<filename<<" , ERRORNO IS "<<(int)res_lookup.unwrap_error()<<std::endl;
        }
        auto inode_id = res_lookup.unwrap();
        auto type_res = chfs_client->get_type_attr(inode_id);
        if(type_res.is_err()){
            std::cerr<<"ERROR IN GETCONT(GETATTR) WITH FILE "<<filename<<" , ERRORNO IS "<<(int)type_res.unwrap_error()<<std::endl;
        }
        auto file_type = type_res.unwrap();
        auto file_size = file_type.second.size;
        auto read_res = chfs_client->read_file(inode_id, 0, file_size);
        if(read_res.is_err()){
            std::cerr<<"ERROR IN GETCONT(READ) WITH FILE "<<filename<<" , ERRORNO IS "<<(int)read_res.unwrap_error()<<std::endl;
        }
        auto content_vec = read_res.unwrap();
        std::string content = std::string(content_vec.begin(), content_vec.end());
        return content;
    }
    std::vector<KeyVal> Worker::readFile(std::string filename){
        auto content = getContentFromFile(filename);
        std::vector<KeyVal>ret;
        
        std::string key,val;
        for(char c : content){
            if(c =='\n' || c == '\0'){
                if(!key.empty() && !val.empty())ret.push_back(KeyVal(key,val));
                key = val = "";
            }
            if(std::isalpha(c)){
                key+=c;
            }
            if(std::isdigit(c)){
                val+=c;
            }
        }
        if(!key.empty() && !val.empty())
            ret.push_back(KeyVal(key,val));
        return ret;
    }
    
    void Worker::writeFile(std::string filename, std::vector<KeyVal>kvs){
        std::string content = "";
        for(KeyVal kv : kvs){
            content += kv.key + " " + kv.val + "\n";
        }

        std::vector<chfs::u8> cont_vec;
        for (char c : content) {
            cont_vec.push_back(static_cast<uint8_t>(c));
        }

        auto lookup_res = chfs_client->lookup(1, filename);
        if(lookup_res.is_err()){
            std::cerr<<"ERROR IN WRITEFILE(SAVING OUTPUT) WITH FILE "<<filename<<" , ERRORNO IS "<<(int)lookup_res.unwrap_error()<<std::endl;
        }
        auto output_inode_id = lookup_res.unwrap();
        auto write_res = chfs_client->write_file(output_inode_id, 0, cont_vec);
        if(write_res.is_err()){
            std::cerr<<"ERROR IN WRITEFILE(WRITING TO OUTPUT) WITH FILE "<<filename<<" , ERRORNO IS "<<(int)write_res.unwrap_error()<<std::endl;
        }
    }

    void Worker::doMap(int index, const std::string &filename) {
        if(verbose)printf("WORKER:do map %s\n", filename.c_str());
        auto content = getContentFromFile(filename);
        auto mapres = Map(content);
        chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, task.outputFile);
        writeFile(task.outputFile, mapres);
        doSubmit(MAP, index);
    }

    void Worker::doReduce(int index, int nfiles) {
        // Lab4: Your code goes here.
        chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, task.outputFile);
        std::vector<KeyVal>kvs, res_kvs;
        for(auto file : task.files){
            auto kv = readFile(file);
            kvs.insert(kvs.end(), kv.begin(), kv.end());
        }
        sort(kvs.begin(),kvs.end());
        
        size_t start_idx = 0, end_idx = 0, total_idx = kvs.size();
        while(end_idx < total_idx){
            std::vector<std::string>values;
            std::string key = kvs[start_idx].key;
            while(end_idx < total_idx - 1 && kvs[end_idx] == kvs[end_idx + 1])
                values.push_back(kvs[end_idx++].val);
            values.push_back(kvs[end_idx].val);
            std::string reduce_res = Reduce(key, values);
            res_kvs.push_back(KeyVal(key, reduce_res));
            start_idx = ++end_idx;
        }
        writeFile(task.outputFile, res_kvs);
        doSubmit(REDUCE, index);
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        // Lab4: Your code goes here.
        mr_client->call(SUBMIT_TASK, (int)taskType, index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            // Lab4: Your code goes here.
            auto reply = mr_client->call(ASK_TASK, 0);
            if(reply.is_err()){
    			std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
            auto content = reply.unwrap()->as<AskTaskReply>();
            auto res = content.res;
            task.outputFile = content.outputFile;
            task.files = content.files;
            if(res == NoTask)break;
            if(res == Busy){
    			std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            if(res == MapTask){
                doMap(content.index, task.files[0]);
                continue;
            }
            if(res == ReduceTask){
                doReduce(content.index, task.files.size());
                continue;
            }

        }
    }
}