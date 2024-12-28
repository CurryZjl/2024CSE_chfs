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
        // if(verbose)printf("CORD:being asked.\n");
        AskTaskReply reply;
        reply.index = 0, reply.res = NoTask;
        if(isFinished)
            return reply;
        std::unique_lock<std::mutex> uniqueLock(mtx);
        if(!workStage){
            for(int i = 0; i < mapFileCnt; i++){
                if(MapTasks[i].finished)continue;
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - MapTasks[i].startTime);
        // if(verbose)printf("CORD:checking map %s.\n", MapTasks[i].files[0].c_str());
                if(!MapTasks[i].hasAligned || duration >= threhold){
                    if(verbose)printf("CORD:aligned map task %d/%d.\n",i, mapFileCnt);
                    reply.res = MapTask;
                    reply.outputFile = MapTasks[i].outputFile;
                    reply.files = MapTasks[i].files;
                    reply.index = i;
                    MapTasks[i].startTime = std::chrono::high_resolution_clock::now();
                    MapTasks[i].hasAligned = true;
                    return reply;
                }
            }
            reply.res = Busy;
            return reply;
        }
        if(workStage == 1){
                // if(verbose)printf("CORD:preparing reduce %d\n",reduceFileCnt);
            for(int i = 0; i < reduceFileCnt; i++){
                if(ReduceTasks[i].finished)continue;
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - ReduceTasks[i].startTime);
                if(!ReduceTasks[i].hasAligned || duration >= threhold){
                    if(verbose)printf("CORD:aligned reduce task %d/%d.\n",i, reduceFileCnt);
                    reply.res = ReduceTask;
                    reply.outputFile = ReduceTasks[i].outputFile;
                    reply.files = ReduceTasks[i].files;
                    reply.index = i;
                    ReduceTasks[i].startTime = std::chrono::high_resolution_clock::now();
                    ReduceTasks[i].hasAligned = true;
                    return reply;
                }
            }
            reply.res = Busy;
            return reply;
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - FinalTask.startTime);
        if(!FinalTask.hasAligned || duration >= threhold){
            if(verbose)printf("CORD:aligned Finaltask.\n");
            reply.res = ReduceTask;
            reply.outputFile = FinalTask.outputFile;
            reply.files = FinalTask.files;
            reply.index = -1;
            FinalTask.startTime = std::chrono::high_resolution_clock::now();
            FinalTask.hasAligned = true;
            return reply;
        }
        reply.res = Busy;
        return reply;
    }

    int Coordinator::submitTask(int taskType, int index) {
        // Lab4 : Your code goes here.
        if(isFinished)return 0;
        std::unique_lock<std::mutex> uniqueLock(mtx);
        if(taskType == MAP){
            if(verbose)printf("CORD:got submission map task %d.\n",index);
            if(MapTasks[index].finished)return 0;
            MapTasks[index].finished = true;
            mapCompCnt ++;
            if(verbose)printf("CORD:finished map task %d/%d.\n",mapCompCnt, mapFileCnt);
            if(mapCompCnt == mapFileCnt)workStage++;
        }
        else if(taskType == REDUCE){
            if(verbose)printf("CORD:got submission map task %d.\n",index);
            if(index >= 0){
                if(ReduceTasks[index].finished)return 0;
                ReduceTasks[index].finished = true;
                reduceCompCnt++;
            if(verbose)printf("CORD:finished reduce task %d/%d.\n",reduceCompCnt, reduceFileCnt);
                if(reduceCompCnt == reduceFileCnt)workStage++;
            }
            else{
            if(verbose)printf("CORD:got submission final task\n");
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
        mapFileCnt = files.size();
        // Lab4: Your code goes here (Optional).
    
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
            if(i+2 == mapFileCnt-1){
                task.files.push_back("map"+std::to_string(i+2));
                i+=3;
            }
            task.outputFile = "reduce"+std::to_string(i/2);
            ReduceTasks.push_back(task);
            reduceFileCnt++;
            FinalTask.files.push_back(task.outputFile);
        }
        FinalTask.outputFile = config.resultFile;
    }
}