#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    AskTaskReply Coordinator::askTask(int) {
        // Lab4 : Your code goes here.
        // Free to change the type of return value.
        AskTaskReply reply;
        reply.index = 0;
        reply.res = NoTask;
        if(isFinished){
            return reply;
        }

        std::unique_lock<std::mutex> uniqueLock(mtx);
        auto now = std::chrono::high_resolution_clock::now();

        // 使用一个辅助函数来处理任务
        auto processTasks = [&](auto& tasks, int stage) {
            for (int i = 0; i < mapFileCnt; i++) {
                if (tasks[i].finished) {
                    continue;
                }

                auto taskTime = std::chrono::duration_cast<std::chrono::milliseconds>(now - tasks[i].startTime);
                if (!tasks[i].hasAligned || taskTime >= threhold) {
                    reply.res = stage == 0 ? MapTask : ReduceTask;
                    reply.index = stage == 0 ? i : -1;  // Map stage gives actual index; Reduce stage gives -1
                    reply.outputFile = tasks[i].outputFile;
                    reply.files = tasks[i].files;
                    tasks[i].startTime = now; // 更新为当前时间
                    tasks[i].hasAligned = true;
                    return true; // 表示找到一个可用的任务
                }
            }
            return false; // 没有找到可用任务
        };

        if (workStage == 0) {
            if (processTasks(MapTasks, 0)) {
                return reply;
            }
        } else if (workStage == 1) {
            if (processTasks(ReduceTasks, 1)) {
                return reply;
            }
        }

        now =  std::chrono::high_resolution_clock::now();
        auto taskTime = std::chrono::duration_cast<std::chrono::milliseconds>(now - FinalTask.startTime);
        if(!FinalTask.hasAligned || taskTime >= threhold){
            reply.res = ReduceTask;
            reply.index = -1;
            reply.outputFile = FinalTask.outputFile;
            reply.files = FinalTask.files;
            FinalTask.startTime = std::chrono::high_resolution_clock::now();
            FinalTask.hasAligned = true;
            return reply;
        }
        reply.res = Busy;
        return reply;
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        if(isFinished)
            return 0;
        std::unique_lock<std::mutex> uniqueLock(mtx);
        if(taskType == MAP){
           
            if(MapTasks[index].finished)
                return 0;
            MapTasks[index].finished = true;
            mapCompCnt++;
        
            if(mapCompCnt == mapFileCnt)
                workStage++;
        }
        else if(taskType == REDUCE){
            if(index >= 0){
                if(ReduceTasks[index].finished)
                    return 0;
                ReduceTasks[index].finished = true;
                reduceCompCnt++;
                if(reduceCompCnt == reduceFileCnt)
                    workStage++;
            }
            else{
                FinalTask.finished = true;
                isFinished = true;
            }
        }
        return 1;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        // Lab4: Your code goes here (Optional).
        mapFileCnt = files.size();
    
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);

        for(int i = 0; i < mapFileCnt; i++){
            auto file = files[i];
            Task task(MAP);
            task.files.push_back(file);
            task.outputFile = "map"+std::to_string(i);
            MapTasks.push_back(task);
        }
        for(int i = 0; i < mapFileCnt; i+=2){
            Task task(REDUCE);
            task.files.push_back("map"+std::to_string(i));
            task.files.push_back("map"+std::to_string(i+1));
            if(i + 2 == mapFileCnt - 1){
                task.files.push_back("map"+std::to_string(i+2));
                i += 3;
            }
            task.outputFile = "reduce"+std::to_string(i/2);
            ReduceTasks.push_back(task);
            reduceFileCnt++;
            FinalTask.files.push_back(task.outputFile);
        }
        FinalTask.outputFile = config.resultFile;
    }
}