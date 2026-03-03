#ifndef _TASKSYS_H
#define _TASKSYS_H

#include "itasksys.h"
#include <queue>
#include <unordered_map>
#include <unordered_set>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

/*
 * TaskSystemSerial: This class is the student's implementation of a
 * serial task execution engine.  See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */
class TaskSystemSerial: public ITaskSystem {
    public:
        TaskSystemSerial(int num_threads);
        ~TaskSystemSerial();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelSpawn: This class is the student's implementation of a
 * parallel task execution engine that spawns threads in every run()
 * call.  See definition of ITaskSystem in itasksys.h for documentation
 * of the ITaskSystem interface.
 */
class TaskSystemParallelSpawn: public ITaskSystem {
    public:
        TaskSystemParallelSpawn(int num_threads);
        ~TaskSystemParallelSpawn();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSpinning: This class is the student's
 * implementation of a parallel task execution engine that uses a
 * thread pool. See definition of ITaskSystem in itasksys.h for
 * documentation of the ITaskSystem interface.
 */
class TaskSystemParallelThreadPoolSpinning: public ITaskSystem {
    public:
        TaskSystemParallelThreadPoolSpinning(int num_threads);
        ~TaskSystemParallelThreadPoolSpinning();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void sync();
};

/*
 * TaskSystemParallelThreadPoolSleeping: This class is the student's
 * optimized implementation of a parallel task execution engine that uses
 * a thread pool. See definition of ITaskSystem in
 * itasksys.h for documentation of the ITaskSystem interface.
 */

 struct TaskRecord {
    TaskID id;
    IRunnable* runnable;
    int current_task_index;
    int current_completed_tasks;
    int num_total_tasks;
    size_t num_dependencies;

    std::vector<TaskID> dependent_tasks;
};

class TaskSystemParallelThreadPoolSleeping: public ITaskSystem {
    private:
        int num_threads;
        std::atomic<bool> killed{false};
        std::vector<std::thread> thread_pool;
        std::atomic<int> task_id_counter;
        


        std::mutex mtx;
        std::mutex ready_tasks_mtx;
        std::unordered_map<TaskID, std::vector<TaskID>> book_keeper;
        std::queue<TaskRecord*> ready_tasks;
        std::unordered_map<TaskID, TaskRecord*> waiting_tasks;
        std::unordered_set<TaskID> completed_tasks;

        std::condition_variable wait_for_tasks_cv;
        std::condition_variable wait_for_sync_cv;
    public:
        TaskSystemParallelThreadPoolSleeping(int num_threads);
        ~TaskSystemParallelThreadPoolSleeping();
        const char* name();
        void run(IRunnable* runnable, int num_total_tasks);
        TaskID runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                const std::vector<TaskID>& deps);
        void workerThreadFunc();
        void sync();
};

#endif
