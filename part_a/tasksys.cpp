#include <stdio.h>

#include "tasksys.h"
#include "CycleTimer.h"

IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }
    // printf("Running Parallel Spawn with %d tasks\n", num_total_tasks);
    std::vector<std::thread> workers;
    for(int i = 0; i < this->num_threads; i++) {
        workers.emplace_back([runnable, num_total_tasks, i, num_threads=this->num_threads](){
            for(int j = i; j < num_total_tasks; j += num_threads) {
                runnable->runTask(j, num_total_tasks);
            }
        });
    }
    for(auto& worker : workers) {
        worker.join();
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    this->kill = false;

    for(int i = 0; i < num_threads; i++) {
        this->thread_pool.emplace_back([this](){
            while(true) {
                if(this->kill) return;

                TaskWithId task;
                bool has_task = false;

                {
                    std::lock_guard<std::mutex> lock(this->mtx);
                    if(!this->tasks.empty()) {
                        task = this->tasks.front();
                        this->tasks.pop();
                        has_task = true;
                    }
                }

                if(has_task) {
                    
                    task.task->runTask(task.id, task.num_total_tasks);

                    {
                        std::lock_guard<std::mutex> lock(this->mtx);
                        this->task_counter--;
                        if(this->task_counter == 0) {
                            this->finished = true;
                        }
                    }
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    {
        std::lock_guard<std::mutex> lock(this->mtx);
        this->kill = true;
    }

    for(auto& t : this->thread_pool) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    {
        std::lock_guard<std::mutex> lock(this->mtx);
        this->task_counter = num_total_tasks;
        this->finished = false;
        for (int i = 0; i < num_total_tasks; i++) {
            this->tasks.push(TaskWithId{i, num_total_tasks, runnable});
        }
    }

    while(this->finished == false) {
        // spin
        std::this_thread::yield();
    }

    // {
    //     std::lock_guard<std::mutex> lock(this->mtx);
    //     if(this->tasks.empty()){
    //         printf("the %d bulk task, all tasks completed\n", bulk_times++);;
    //     } else {
    //         printf("Error: tasks still in queue after task_counter reached 0, left %ld tasks\n", this->tasks.size());
    //     }
    // }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->num_threads = num_threads;
    for(int i = 0; i < num_threads; i++) {
        this->thread_pool.emplace_back([this](){
            while(true) {
                TaskWithId task;
                {
                    std::unique_lock<std::mutex> lock(this->mtx_);
                    this->has_task_cv.wait(lock, [this](){
                        return this->kill || !this->tasks.empty();
                    });

                    if(this->kill) return;
                    if(!this->tasks.empty()) {
                        task = this->tasks.front();
                        this->tasks.pop();
                    } else {
                        continue;
                    }
                }

                task.task->runTask(task.id, task.num_total_tasks);
                
                bool finished = false;
                {
                    std::lock_guard<std::mutex> lock(this->mtx_);
                    this->task_counter--;
                    if(this->task_counter == 0) {
                        finished = true;
                    }
                }
                if(finished) {
                    this->finished_cv.notify_all();
                }
            }
        });
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    {
        std::lock_guard<std::mutex> lock(this->mtx_);
        this->kill = true;
    }
    this->has_task_cv.notify_all();
    this->finished_cv.notify_all();
    for(auto& t : this->thread_pool) {
        t.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    {
        std::lock_guard<std::mutex> lock(this->mtx_);
        this->task_counter = num_total_tasks;
        for (int i = 0; i < num_total_tasks; i++) {
            this->tasks.push(TaskWithId{i, num_total_tasks, runnable});
        }
    }
    this->has_task_cv.notify_all();
    {
        std::unique_lock<std::mutex> lock(this->mtx_);
        this->finished_cv.wait(lock, [this](){
            return this->task_counter == 0;
        });
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
