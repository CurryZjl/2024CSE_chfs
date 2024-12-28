#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <unordered_map>

#include <thread>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
        // Lab4: Your code goes here (Optional).
    }

    void Worker::doMap(int index, const std::string &filename) {
        // Lab4: Your code goes here.
        auto lookupRes = chfs_client->lookup(1, filename);
        if(lookupRes.is_err()){
            std::cerr << "error in lookup of" << filename << " detail:" << (int)lookupRes.unwrap_error() << std::endl;
            return;
        }
        auto inodeId = lookupRes.unwrap();
        auto type = chfs_client->get_type_attr(inodeId);
        if(type.is_err()){
            std::cerr << "error in get_type_attr " << inodeId << " detail:" << (int)type.unwrap_error() << std::endl;
            return;
        }
        auto file_type = type.unwrap();
        auto file_size = file_type.second.size;
        auto readRes = chfs_client->read_file(inodeId, 0, file_size);
        if(readRes.is_err()){
            std::cerr << "error in read_file " << inodeId << " detail:" << (int)readRes.unwrap_error() << std::endl;
            return;
        }
        auto content_vec = readRes.unwrap();
        std::string content(content_vec.begin(), content_vec.end());

        auto mapRes = Map(content);
        chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, task.outputFile);
        //writeFile(task.outputFile, mapRes);
        std::ostringstream oss;
        for (const KeyVal &kv : mapRes) {
            oss << kv.key << " " << kv.val << "\n";
        }
        content = oss.str();

        std::vector<chfs::u8> charVector(content.begin(), content.end());

        lookupRes = chfs_client->lookup(1, task.outputFile);
        if(lookupRes.is_err()){
           std::cerr << "error in lookup " << outPutFile << " detail:" << (int)lookupRes.unwrap_error() << std::endl;
        }
        auto outputInodeId = lookupRes.unwrap();
        auto writeRes = chfs_client->write_file(outputInodeId, 0, charVector);
        if(writeRes.is_err()){
            std::cerr << "error in write_file " << outputInodeId << " detail:" << (int)writeRes.unwrap_error() << std::endl;
        }
        doSubmit(MAP, index);
    }

    void Worker::doReduce(int index, int nfiles) {
        // Lab4: Your code goes here.
        chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, task.outputFile);
        std::vector<KeyVal>kvs, res_kvs;
        for(const auto &file : task.files){
            auto kv = readFile(file);
            kvs.insert(kvs.end(), kv.begin(), kv.end());
        }
        
        sort(kvs.begin(),kvs.end());
        
        size_t size = kvs.size();
        // while(end_i < size){
        //     std::vector<std::string>values;
        //     std::string key = kvs[start_i].key;
        //     while((end_i < size - 1) && (kvs[end_i].key == kvs[end_i + 1].key))
        //         values.push_back(kvs[end_i++].val);
        //     values.push_back(kvs[end_i].val);
        //     std::string reduce_res = Reduce(key, values);
        //     res_kvs.push_back(KeyVal(key, reduce_res));
        //     start_i = ++end_i;
        // }
        for(size_t start_i = 0; start_i < size; ){
            std::vector<std::string> values;
            std::string key = kvs[start_i].key;

            while(start_i < size && kvs[start_i].key == key){
                values.push_back(std::move(kvs[start_i++].val));
            }
            std::string reduce_res = Reduce(key, values);
            res_kvs.emplace_back(std::move(key), std::move(reduce_res));
        }

        std::ostringstream oss;
        for (const KeyVal& kv : res_kvs) {
            oss << kv.key << " " << kv.val << "\n";
        }

        std::string content = oss.str();

        std::vector<chfs::u8> charVector(content.begin(), content.end());
        
        auto lookupRes = chfs_client->lookup(1, task.outputFile);
        if(lookupRes.is_err()){
           std::cerr << "error in lookup " << outPutFile << " detail:" << (int)lookupRes.unwrap_error() << std::endl;
        } else {
            auto outputInodeId = lookupRes.unwrap();
            auto writeRes = chfs_client->write_file(outputInodeId, 0, charVector);
            if(writeRes.is_err()){
                std::cerr << "error in write_file " << outputInodeId << " detail:" << (int)writeRes.unwrap_error() << std::endl;
            }
        }
        
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
    			std::this_thread::sleep_for(std::chrono::milliseconds(40));
                continue;
            }
            auto content = reply.unwrap()->as<AskTaskReply>();
            auto res = content.res;
            task.outputFile = content.outputFile;
            task.files = content.files;

            switch (res)
            {
                case NoTask:
                    break;
                case Busy:
                    std::this_thread::sleep_for(std::chrono::milliseconds(40));
                    break;
                case MapTask:
                    doMap(content.index, task.files[0]);
                    break;
                case ReduceTask:
                    doReduce(content.index, task.files.size());
                    break;
                default:
                    break;
            }
        }
    }

    std::vector<KeyVal> Worker::readFile(std::string filename){
        auto lookupRes = chfs_client->lookup(1, filename);
        if(lookupRes.is_err()){
            std::cerr << "error in lookup of" << filename << " detail:" << (int)lookupRes.unwrap_error() << std::endl;
            return {};
        }
        auto inodeId = lookupRes.unwrap();
        auto type = chfs_client->get_type_attr(inodeId);
        if(type.is_err()){
            std::cerr << "error in get_type_attr " << inodeId << " detail:" << (int)type.unwrap_error() << std::endl;
            return {};
        }
        auto file_type = type.unwrap();
        auto file_size = file_type.second.size;
        auto readRes = chfs_client->read_file(inodeId, 0, file_size);
        if(readRes.is_err()){
            std::cerr << "error in read_file " << inodeId << " detail:" << (int)readRes.unwrap_error() << std::endl;
        }
        auto content_vec = readRes.unwrap();
        std::ostringstream oss;
        for(char c : content_vec){
            oss << c;
        }
        std::string content = oss.str();
        std::vector<KeyVal>ret;
        std::string key,val;
        for(char &c : content){
            if(c =='\n' || c == '\0'){
                if(!key.empty() && !val.empty())
                    ret.emplace_back(key,val);
                key.clear();
                val.clear();
            }
            else if(std::isalpha(c)){
                key += c;
            }
            else if(std::isdigit(c)){
                val += c;
            }
        }
        if(!key.empty() && !val.empty())
            ret.emplace_back(key,val);
        return ret;
    }
    
}