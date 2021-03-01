#pragma once
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>

template <typename T>
class ConnPool {
public:
    ~ConnPool() { }
    ConnPool() { }
    void set_timeout(int timeout) { timeout_ = timeout * 1000000; }
    void put(T& c) { send(c); }
    std::pair<bool, T> get()
    {
        if (timeout_ < 0)
            return { true, recv() };
        auto c = recv(timeout_);
        if (!c.has_value())
            return { false, nullptr };
        return { true, c.value() };
    }
    void send(T& item)
    {
        {
            std::lock_guard<std::mutex> lock(lk_);
            conns_.push(item);
        }
        cv_.notify_one();
    }
    T recv()
    {
        std::unique_lock<std::mutex> lock(lk_);
        cv_.wait(lock, [this] { return !conns_.empty(); });
        auto res = std::move(conns_.front());
        conns_.pop();
        return res;
    }
    std::optional<T> recv(int timeout_in_us)
    {
        std::unique_lock<std::mutex> lock(lk_);
        if (cv_.wait_for(lock, std::chrono::microseconds(timeout_in_us), [this] { return !conns_.empty(); })) {
            auto res = std::move(conns_.front());
            conns_.pop();
            return res;
        }
        return std::nullopt;
    }

private:
    std::queue<T> conns_;
    std::mutex lk_;
    std::condition_variable cv_;
    int timeout_ = -1;
};