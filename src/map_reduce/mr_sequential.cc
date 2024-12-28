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
        // if(verbose)printf("starting dowork\n");
        // Your code goes here
        std::vector<KeyVal> kvs;
        std::vector<KeyVal> res_kvs;
        for(const std::string &file : files){
        // if(verbose)printf("trying to read file %s\n", file.c_str());
            auto res_lookup = chfs_client->lookup(1, file);
            if(res_lookup.is_err()){
                std::cerr<<"ERROR IN DOWORK(LOOKUP) WITH FILE "<<file<<" , ERRORNO IS "<<(int)res_lookup.unwrap_error()<<std::endl;
            }
            auto inode_id = res_lookup.unwrap();
            auto type_res = chfs_client->get_type_attr(inode_id);
            if(type_res.is_err()){
                std::cerr<<"ERROR IN DOWORK(GETATTR) WITH FILE "<<file<<" , ERRORNO IS "<<(int)type_res.unwrap_error()<<std::endl;
            }
            auto file_type = type_res.unwrap();
            auto file_size = file_type.second.size;
            auto read_res = chfs_client->read_file(inode_id, 0, file_size);
            if(read_res.is_err()){
                std::cerr<<"ERROR IN DOWORK(READ) WITH FILE "<<file<<" , ERRORNO IS "<<(int)read_res.unwrap_error()<<std::endl;
            }
            auto content_vec = read_res.unwrap();
            std::string content = std::string(content_vec.begin(), content_vec.end());
        // if(verbose)printf("get content from file %s is\n%s\n", file.c_str(), content.c_str());
            auto mapres = Map(content);
            kvs.insert(kvs.end(), mapres.begin(), mapres.end());
        }

        // sort(kvs.begin(), kvs.end());
        for(int i = 0;i < kvs.size(); i++){
            for(int j = i+1; j < kvs.size(); j++){
                if(kvs[j] < kvs [i])std::swap(kvs[i], kvs[j]);
            }
        }

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

        std::string write_content = "";
        for(const auto &res_kv : res_kvs){
            write_content += res_kv.key + " " + res_kv.val + "\n";
        }
        std::vector<chfs::u8> cont_vec;
        for (char c : write_content) {
            cont_vec.push_back(static_cast<uint8_t>(c));
        }

        auto lookup_res = chfs_client->lookup(1, outPutFile);
        if(lookup_res.is_err()){
            std::cerr<<"ERROR IN DOWORK(SAVING OUTPUT) WITH FILE "<<outPutFile<<" , ERRORNO IS "<<(int)lookup_res.unwrap_error()<<std::endl;
        }
        auto output_inode_id = lookup_res.unwrap();
        auto write_res = chfs_client->write_file(output_inode_id, 0, cont_vec);
        if(write_res.is_err()){
            std::cerr<<"ERROR IN DOWORK(WRITING TO OUTPUT) WITH FILE "<<outPutFile<<" , ERRORNO IS "<<(int)write_res.unwrap_error()<<std::endl;
        }
    }
}