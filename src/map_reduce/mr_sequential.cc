#include <string>
#include <utility>
#include <vector>
#include <algorithm>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
        // Your code goes here (optional)
    }

    void SequentialMapReduce::doWork() {
        // Your code goes here
        std::vector<KeyVal> kvs;

        //map
        for(const std::string &file : files){
            auto lookupRes = chfs_client->lookup(1, file);
            if(lookupRes.is_err()){
                std::cerr << "error in lookup of" << file << " detail:" << (int)lookupRes.unwrap_error() << std::endl;
            }
            auto inodeId = lookupRes.unwrap();
            auto type = chfs_client->get_type_attr(inodeId);
            if(type.is_err()){
                std::cerr << "error in get_type_attr " << inodeId << " detail:" << (int)type.unwrap_error() << std::endl;
            }
            auto fileType = type.unwrap();
            auto fileSize = fileType.second.size;
            auto readRes = chfs_client->read_file(inodeId, 0, fileSize);
            if(readRes.is_err()){
                std::cerr << "error in read_file " << inodeId << " detail:" << (int)readRes.unwrap_error() << std::endl;
            }
            auto content_res = readRes.unwrap();
            std::string content = std::string(content_res.begin(), content_res.end());

            auto map_ = Map(content);
            kvs.insert(kvs.end(), map_.begin(), map_.end());
        }

        //sort kvs
        size_t size = kvs.size();
        for(size_t i = 0; i < size; i++){
            for(size_t j = i+1; j < size; j++){
                if(kvs[j].key < kvs[i].key)
                    std::swap(kvs[i], kvs[j]);
            }
        }

        //reduce
        std::vector<KeyVal> res;
        size_t start_i = 0, end_i = 0;
        while(end_i < size){
            std::vector<std::string> values;
            std::string key = kvs[start_i].key;
            while(end_i < (size -1) && kvs[end_i].key == kvs[end_i +1].key)
                values.push_back(kvs[end_i++].val);
            values.push_back(kvs[end_i].val);
            std::string reduceRes = Reduce(key, values);
            res.push_back(KeyVal(key, reduceRes));
            start_i = ++end_i;
        }

        //store res
        std::string writeContent = "";
        for(const KeyVal &kv : res){
            writeContent += kv.key + " " + kv.val + "\n";
        }
        std::vector<chfs::u8> charVector;
        for(const char c : writeContent) {
            charVector.push_back(c);
        }
        auto lookupRes = chfs_client->lookup(1, outPutFile);
        if(lookupRes.is_err()){
            std::cerr << "error in lookup " << outPutFile << " detail:" << (int)lookupRes.unwrap_error() << std::endl;
        }
        auto outputInodeId = lookupRes.unwrap();
        auto writeRes = chfs_client->write_file(outputInodeId, 0, charVector);
        if(writeRes.is_err()){
            std::cerr << "error in write_file " << outputInodeId << " detail:" << (int)writeRes.unwrap_error() << std::endl;
        }

    }
}