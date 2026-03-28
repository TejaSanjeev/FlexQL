#ifndef FLEXQL_THREAD_POOL_H
#define FLEXQL_THREAD_POOL_H

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>

namespace flexql {
namespace concurrency {

class ThreadPool {
public:
    // Initialize the pool with a specific number of worker threads
    explicit ThreadPool(size_t num_threads);
    ~ThreadPool();

    // Enqueue a task (e.g., handling a client connection)
    void enqueue(std::function<void()> task);

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;

    std::mutex queue_mutex_;
    std::condition_variable condition_;
    std::atomic<bool> stop_;
};

} // namespace concurrency
} // namespace flexql

#endif // FLEXQL_THREAD_POOL_H