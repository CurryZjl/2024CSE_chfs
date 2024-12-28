#include <string>
#include <utility>
#include <vector>
#include <mutex>
#include "librpc/client.h"
#include "librpc/server.h"
#include "distributed/client.h"

//Lab4: Free to modify this file

namespace mapReduce {
    struct KeyVal {
        KeyVal(const std::string &key, const std::string &val) : key(key), val(val) {}
        KeyVal(){}
        std::string key;
        std::string val;
        bool operator <(const KeyVal &kv2) const{
            return key < kv2.key;
        }
        bool operator ==(const KeyVal &kv2) const{
            return key == kv2.key;
        }
    };

    enum mr_tasktype {
        NONE = 0,
        MAP,
        REDUCE
    };

    std::vector<KeyVal> Map(const std::string &content);

    std::string Reduce(const std::string &key, const std::vector<std::string> &values);

    const std::string ASK_TASK = "ask_task";
    const std::string SUBMIT_TASK = "submit_task";

    struct MR_CoordinatorConfig {
        uint16_t port;
        std::string ip_address;
        std::string resultFile;
        std::shared_ptr<chfs::ChfsClient> client;

        MR_CoordinatorConfig(std::string ip_address, uint16_t port, std::shared_ptr<chfs::ChfsClient> client,
                             std::string resultFile) : port(port), ip_address(std::move(ip_address)),
                                                       resultFile(resultFile), client(std::move(client)) {}
    };

    class SequentialMapReduce {
    public:
        SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client, const std::vector<std::string> &files, std::string resultFile);
        void doWork();
        bool verbose = false;

    private:
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::vector<std::string> files;
        std::string outPutFile;
    };

    class Task{
    public:
        mr_tasktype taskType;
        std::vector<std::string>files;
        std::string outputFile;

        std::chrono::_V2::system_clock::time_point startTime;
        bool finished = false;
        bool hasAligned = false;

        Task(){}
        Task(mr_tasktype taskt):taskType(taskt){}
    };
    enum askRes{
        NoTask, MapTask, ReduceTask, Busy
    };
    struct AskTaskReply {
        int res;
        int index;
        std::string outputFile;
        std::vector<std::string>files;
        MSGPACK_DEFINE(
            res,index,outputFile,files
        )
    };


    class Coordinator {
    public:
        Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce);
        AskTaskReply askTask(int);
        int submitTask(int taskType, int index);
        bool Done();
        

    private:
        std::vector<std::string> files;
        std::mutex mtx;
        bool isFinished;
        std::unique_ptr<chfs::RpcServer> rpc_server;

        std::vector<Task> MapTasks, ReduceTasks;
        int mapFileCnt = 0, mapCompCnt = 0;
        int reduceFileCnt = 0, reduceCompCnt = 0;
        Task FinalTask = Task(REDUCE);

        int workStage = 0;
        std::chrono::_V2::system_clock::duration threhold = std::chrono::duration_cast<std::chrono::system_clock::duration>(std::chrono::milliseconds(1000));

        bool verbose = false;
    };

    class Worker {
    public:
        explicit Worker(MR_CoordinatorConfig config);
        void doWork();
        void stop();

    private:    
        bool verbose = false;

        void doMap(int index, const std::string &filename);
        void doReduce(int index, int nfiles);
        void doSubmit(mr_tasktype taskType, int index);
        std::vector<KeyVal> readFile(std::string filename);
        void writeFile(std::string filename, std::vector<KeyVal>kvs);
        std::string getContentFromFile(std::string filename);

        std::string outPutFile;
        std::unique_ptr<chfs::RpcClient> mr_client;
        std::shared_ptr<chfs::ChfsClient> chfs_client;
        std::unique_ptr<std::thread> work_thread;

        std::vector<std::string>reduceFiles;
        bool shouldStop = false;

        Task task;
    };
}