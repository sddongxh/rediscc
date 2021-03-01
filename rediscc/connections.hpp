#pragma once
#include <iostream>
#include <memory>
#include <string>

#include "unistd.h"
extern "C" {
#include <hiredis/hiredis.h>
#include <hiredis_cluster/hircluster.h>
}
// Redis types
enum {
    REDIS_CLUSTER,
    REDIS_SINGLE,
};

struct RedisOptions {
    std::string IP = "127.0.0.1";
    int Port = 6379;
    int Type = REDIS_SINGLE;
    std::string Password;
    int DialTimeout = 10; //second
    int MaxRetries = 3; // Default is 3 retries for commands

    bool UsePool = false;
    int PoolSize = 3;
    int PoolTimeout = 10; // second
    int Flags = 0; // HIRCLUSTER_FLAG_ROUTE_USE_SLOTS; // only works for cluster
};

class RedisConnections {
public:
    static std::shared_ptr<redisContext> NewConn(const RedisOptions& opt)
    {
        struct timeval tv;
        tv.tv_sec = opt.DialTimeout;
        tv.tv_usec = 0;
        bool KeepAlive = true;

        auto deleter = [](auto c) { redisFree(c); /*std::cout << "redis context released" << std::endl; */ };
        auto c = std::shared_ptr<redisContext>(redisConnectWithTimeout(opt.IP.c_str(), opt.Port, tv), deleter);
        if (c == nullptr || c->err != 0) {
            if (c == nullptr) {
                std::cerr << "Redis: null context" << std::endl;
            } else {
                std::cerr << "Redis: " << c->errstr << std::endl;
            }
            return nullptr;
        }
        if (KeepAlive && redisEnableKeepAlive(c.get()) != REDIS_OK) {
            return nullptr;
        }
        // if (redisSetTimeout(c.get(), { 0, opt.TimeOut }) != REDIS_OK) {
        //     return nullptr;
        // }
        if (!opt.Password.empty()) {
            auto reply = (redisReply*)redisCommand(c.get(), "AUTH %s", opt.Password.c_str());
            if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
                return nullptr;
            }
        }
        return c;
    }
    static std::shared_ptr<redisClusterContext> NewClusterConn(const RedisOptions& opt)
    {
        struct timeval tv;
        tv.tv_sec = opt.DialTimeout;
        tv.tv_usec = 0;
        std::string addrs = opt.IP + ":" + std::to_string(opt.Port);
        auto cc = std::shared_ptr<redisClusterContext>(redisClusterConnectWithTimeout(addrs.c_str(), tv, opt.Flags),
            [](auto c) { redisClusterFree(c); });
        if (cc == nullptr || cc->err != 0) {
            if (cc == nullptr) {
                std::cerr << "Redis: null context" << std::endl;
            } else {
                std::cerr << "Redis: " << cc->errstr << std::endl;
            }
            return nullptr;
        }
        // if (redisClusterSetOptionTimeout(cc.get(), { 0, opt.TimeOut }) != REDIS_OK) {
        //     return nullptr;
        // }
        return cc;
    }
};
