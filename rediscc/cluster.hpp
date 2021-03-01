// Shallow modern C++ wrapper for hiredis/hircluster
// Golang style: https://pkg.go.dev/github.com/go-redis/redis/v8
#pragma once

#include "pool.hpp"
#include "connections.hpp"
#include "cmdable.hpp"
#include "reply.hpp"
#include <iostream>
#include <memory>
#include <optional>
#include <tuple>

class ClusterClient : public RedisCmdable {
public:
    bool Connect(const RedisOptions& opts) { return opts.UsePool ? connectWithPool(opts) : connectWithoutPool(opts); }
    bool is_null() const { return c_ == nullptr; }
    int err() const { return c_->err; }
    void reset_err()
    {
        redisClusterResetErrors(c_.get());
        // c_->err = 0;
    }
    std::string errstr() const { return c_->errstr; }
    redisClusterContext* Context() { return c_.get(); }
    static std::shared_ptr<ClusterClient> NewClient(const RedisOptions& opts)
    {
        std::shared_ptr<ClusterClient> client(new ClusterClient);
        if (client->Connect(opts))
            return client;
        return nullptr;
    }
    bool SetTimeout(const struct timeval& tv)
    {
        return redisClusterSetOptionTimeout(c_.get(), tv) == REDIS_OK;
    }
    std::string GetSrvTag() const { return opts_.IP + ":" + std::to_string(opts_.Port) + ":" + std::to_string(opts_.Type); }

public: // Generic APIs
    // run a redis command with retry
    RedisReply Command(const char* format, ...)
    {
        RedisReply r;
        if (!opts_.UsePool) {
            va_list ap;
            va_start(ap, format);
            r = (redisReply*)redisClustervCommand(c_.get(), format, ap);
            va_end(ap);
            // if redis is disconnected for some reason
            int i = 0;
            while (r.is_null() && i++ < opts_.MaxRetries) {
                std::cerr << "Redis disconnected: reconencting and retrying ... " << std::endl;
                usleep(8000); // backoff 8 milliseconds
                redisClusterReset(c_.get());
                if (c_ != nullptr) {
                    va_list ap;
                    va_start(ap, format);
                    r = (redisReply*)redisClustervCommand(c_.get(), format, ap);
                    va_end(ap);
                }
            }
        } else {
            auto [ok, c] = pool_.get();
            if (!ok) {
                std::cerr << "failed to fetch conn from Redis pool: timeout" << std::endl;
                return r;
            }
            va_list ap;
            va_start(ap, format);
            r = (redisReply*)redisClustervCommand(c.get(), format, ap);
            va_end(ap);

            // if redis is disconnected for some reason
            int i = 0;
            while (r.is_null() && i++ < opts_.MaxRetries) {
                std::cerr << "Redis disconnected: reconencting and retrying ... " << std::endl;
                usleep(8000); // backoff 8 milliseconds
                c = RedisConnections::NewClusterConn(opts_);
                if (c != nullptr) {
                    va_list ap;
                    va_start(ap, format);
                    r = (redisReply*)redisClustervCommand(c.get(), format, ap);
                    va_end(ap);
                }
            }
            pool_.put(c);
        }
        return r;
    }
    bool AppendCommand(const char* format, ...)
    {
        va_list ap;
        va_start(ap, format);
        auto retc = redisClustervAppendCommand(c_.get(), format, ap);
        va_end(ap);
        return retc == REDIS_OK;
    }
    // only works for connection without pooling
    std::pair<bool, RedisReply> GetReply()
    {
        if (opts_.UsePool) {
            std::cerr << "command only available for client without connection pool" << std::endl;
            return { false, RedisReply {} };
        }
        void* reply = nullptr;
        int retc = redisClusterGetReply(c_.get(), &reply);
        RedisReply r = (redisReply*)reply;
        return { retc == REDIS_OK, r };
    }

private:
    std::shared_ptr<redisClusterContext> c_;
    RedisOptions opts_;
    ConnPool<std::shared_ptr<redisClusterContext>> pool_;
    bool connectWithoutPool(const RedisOptions& opts)
    {
        opts_ = opts;
        c_ = RedisConnections::NewClusterConn(opts);
        return c_ != nullptr;
    }
    bool connectWithPool(const RedisOptions& opts)
    {
        opts_ = opts;
        for (int i = 0; i < opts.PoolSize; i++) {
            auto c = RedisConnections::NewClusterConn(opts);
            if (c == nullptr) {
                std::cerr << "failed to creat new connection" << std::endl;
                return false;
            }
            pool_.put(c);
        }
        return true;
    }
};
