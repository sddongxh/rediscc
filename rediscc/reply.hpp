// Shallow Modern C++ wrapper for hiredis redisReply
// Use smart pointer to manage redisReply

#pragma once
extern "C" {
#include <hiredis/hiredis.h>
}
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>

class RedisReply {
public:
    RedisReply() { }
    RedisReply(redisReply* r)
    {
        auto deleter = [](auto p) { freeReplyObject(p); /*cout << "freed" << endl;*/ };
        ptr_ = std::shared_ptr<redisReply>(r, deleter);
    }
    int type() const
    {
        return ptr_->type;
    }
    std::string typestr() const
    {
        return typestrs_.at(type());
    }
    bool is_null()
    {
        return ptr_ == nullptr;
    }
    bool is_nil()
    {
        return ptr_ != nullptr && type() == REDIS_REPLY_NIL;
    }
    bool is_err()
    {
        return ptr_ != nullptr && type() == REDIS_REPLY_ERROR;
    }
    bool is_integer()
    {
        return ptr_ != nullptr && type() == REDIS_REPLY_INTEGER;
    }
    bool is_array()
    {
        return ptr_ != nullptr && type() == REDIS_REPLY_ARRAY;
    }
    bool is_str()
    {
        return ptr_ != nullptr && type() == REDIS_REPLY_STRING;
    }
    bool is_status()
    {
        return ptr_ != nullptr && type() == REDIS_REPLY_STATUS;
    }
    long long integer() const
    {
        return ptr_->integer;
    }
    std::string str() const
    {
        return std::string(ptr_->str, ptr_->len);
    }
    std::vector<std::string> array() const
    {
        std::vector<std::string> strs;
        for (size_t i = 0; i < ptr_->elements; i++) {
            if (ptr_->element[i]->str != nullptr)
                strs.push_back(ptr_->element[i]->str);
        }
        return strs;
    }
    std::unordered_map<std::string, std::string> map() const
    {
        std::unordered_map<std::string, std::string> m;
        std::vector<std::string> strs;
        for (size_t i = 0; i < ptr_->elements; i++) {
            strs.push_back(ptr_->element[i]->str);
            if (i % 2 == 1)
                m[strs[i - 1]] = strs[i];
        }
        return m;
    }
    redisReply* ptr() { return ptr_.get(); }

    // private:
    std::shared_ptr<redisReply> ptr_ = nullptr;
    std::unordered_map<int, std::string> typestrs_ = {
        { 1, "REDIS_REPLY_STRING" },
        { 2, "REDIS_REPLY_ARRAY" },
        { 3, "REDIS_REPLY_INTEGER" },
        { 4, "REDIS_REPLY_NIL" },
        { 5, "REDIS_REPLY_STATUS" },
        { 6, "REDIS_REPLY_ERROR" }
    };
};