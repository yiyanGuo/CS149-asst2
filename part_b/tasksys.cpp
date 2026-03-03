#include "tasksys.h"
#include "itasksys.h"
#include <cassert>
#include <mutex>


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
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemSerial::sync() {
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelSpawn in Part B.
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
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }

    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // NOTE: CS149 students are not expected to implement TaskSystemParallelThreadPoolSpinning in Part B.
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
    this->killed = false;

    for(int i = 0; i < num_threads; i++) {
        this->thread_pool.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerThreadFunc, this);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    //
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
    this->killed = true;

    this->wait_for_tasks_cv.notify_all();
    for(auto& thread: this->thread_pool) {
        thread.join();
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //

    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    // for (int i = 0; i < num_total_tasks; i++) {
    //     runnable->runTask(i, num_total_tasks);
    // }

    TaskID task_id = task_id_counter.fetch_add(1);
    bool has_task_ready = false;

    TaskRecord* task_record = new TaskRecord{task_id, runnable, 0, 0,num_total_tasks, deps.size(), deps};

    {
        std::lock_guard<std::mutex> lk(this->mtx);
        for(TaskID dep: deps) {
            if(this->completed_tasks.find(dep) != this->completed_tasks.end()) {
                task_record->num_dependencies--;
            } else {
                this->book_keeper[dep].push_back(task_id);
            }
        }

        if(task_record->num_dependencies == 0) {
            ready_tasks.push(task_record);
            has_task_ready = true;
        } else {
            this->waiting_tasks[task_id] = task_record;
        }

        // printf("Task %d has %zu dependencies\n", task_id, deps.size());
        // printf("Ready tasks: %zu\n", ready_tasks.size());
    }
    
    if(has_task_ready) {
        this->wait_for_tasks_cv.notify_all();
    }

    return task_id;
}

void TaskSystemParallelThreadPoolSleeping::workerThreadFunc(){
    while(!killed.load()) {
        bool has_task_ready = false;
        bool has_task_completed = false;
        TaskRecord* task_record = nullptr;
        IRunnable* runnable = nullptr;
        int task_index = 0;
        int num_total_tasks = 0;
        
        {
            std::unique_lock<std::mutex> lk(this->ready_tasks_mtx);

            if(ready_tasks.empty()) {
                wait_for_tasks_cv.wait(lk, [this](){ return !ready_tasks.empty() || this->killed.load(); });
            }

            if(this->killed.load()) return;

            task_record = ready_tasks.front();
            runnable = task_record->runnable;
            task_index = task_record->current_task_index++;
            num_total_tasks = task_record->num_total_tasks;
            if(task_index == num_total_tasks - 1) {
                ready_tasks.pop();
            }

            // bool dependent_missing = false;
            // for(TaskID dep: task_record->dependent_tasks) {
            //     if(this->completed_tasks.find(dep) == this->completed_tasks.end()) {
            //         dependent_missing = true;
            //         break;
            //     }
            // }
            // printf("executing task %d , index %d with %d total tasks\n", task_record->id, task_index, num_total_tasks);
        }

        runnable->runTask(task_index, num_total_tasks);
        
        {
            std::lock_guard<std::mutex> lk(this->mtx);
            task_record->current_completed_tasks++;

            if(task_record->current_completed_tasks == num_total_tasks) {
                
                this->completed_tasks.insert(task_record->id);

                for(TaskID dependent_task_id: this->book_keeper[task_record->id]) {
                    TaskRecord* dependent_task_record = this->waiting_tasks[dependent_task_id];
                    dependent_task_record->num_dependencies--;
                    if(dependent_task_record->num_dependencies == 0) {
                        std::lock_guard<std::mutex> ready_lk(this->ready_tasks_mtx);
                        ready_tasks.push(dependent_task_record);
                        has_task_ready = true;
                    }
                }

                has_task_completed = true;
            }
        }


        if(has_task_ready) {
            this->wait_for_tasks_cv.notify_all();
        }
        if(has_task_completed) {
            this->wait_for_sync_cv.notify_all();
        }
    }   
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    {
        std::unique_lock<std::mutex> lk(this->mtx);
        this->wait_for_sync_cv.wait(lk, [this](){ return this->completed_tasks.size() == this->task_id_counter.load(); });
        // printf("checking sync: completed tasks %zu, total tasks %d\n", this->completed_tasks.size(), this->task_id_counter.load());
    }

    return;
}
