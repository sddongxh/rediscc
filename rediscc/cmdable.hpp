// Encloses a set of Redis functions i

#pragma once
#include "reply.hpp"

class RedisCmdable {
   public:
    virtual RedisReply Command(const char* format, ...) = 0;
    virtual bool AppendCommand(const char* format, ...) = 0;
    virtual std::pair<bool, RedisReply> GetReply() = 0;
    virtual bool SetTimeout(const struct timeval& tv) = 0;
    virtual void reset_err() = 0;
    virtual int err() const = 0;
    virtual std::string errstr() const = 0;
    virtual std::string GetSrvTag() const = 0;  // "ip:port"

   public:
    // Customized APIs
    bool Ping() {
        auto r = Command("PING");
        return (!r.is_null()) && (r.is_status()) && (r.str() == "PONG");
    }
    std::pair<bool, int> Del(const std::string& key) {
        auto r = Command("DEL %s", key.c_str());
        if (r.is_null() || !r.is_integer()) {
            return {false, 0};
        }
        return {true, r.integer()};
    }
    bool Set(const std::string& key, const std::string& val) {
        auto r = Command("SET %s %s", key.c_str(), val.c_str());
        return !(r.is_null() || r.is_err());
    }
    std::pair<bool, std::optional<std::string>> Get(const std::string& key) {
        auto r = Command("GET %s", key.c_str());
        if (r.is_nil()) return {true, std::nullopt};
        if (r.is_str()) {
            return {true, r.str()};
        }
        return {false, std::nullopt};
    }
    bool HSet(const std::string& key, const std::string& field, const std::string& val) {
        auto r = Command("HSET %s %s %s", key.c_str(), field.c_str(), val.c_str());
        return !(r.is_null() || r.is_err());
    }
    std::pair<bool, int> HDel(const std::string& key, const std::string& field) {
        auto r = Command("HDEL %s %s", key.c_str(), field.c_str());
        if (r.is_null() || !r.is_integer()) {
            return {false, 0};
        }
        return {true, r.integer()};
    }
    std::pair<bool, std::optional<std::string>> HGet(const std::string& key, const std::string& field) {
        auto r = Command("HGET %s %s", key.c_str(), field.c_str());
        if (r.is_nil()) return {true, std::nullopt};
        if (r.is_str()) return {true, r.str()};
        return {false, std::nullopt};
    }
    std::pair<bool, std::unordered_map<std::string, std::string>> HGetAll(const std::string& key) {
        auto r = Command("HGETALL %s", key.c_str());
        if (r.is_nil()) return {true, std::unordered_map<std::string, std::string>{}};
        if (r.is_array()) return {true, r.map()};
        return {false, std::unordered_map<std::string, std::string>{}};
    }
    bool LPush(const std::string& key, const std::string& val) {
        auto r = Command("LPUSH %s %s", key.c_str(), val.c_str());
        return !(r.is_null() || r.is_err());
    }
    std::pair<bool, std::optional<std::string>> RPop(const std::string& key) {
        auto r = Command("RPOP %s", key.c_str());
        if (r.is_nil()) return {true, std::nullopt};
        if (r.is_str()) return {true, r.str()};
        return {false, std::nullopt};
    }
    std::pair<bool, long long> LLen(const std::string& key) {
        auto r = Command("LLEN %s", key.c_str());
        if (r.is_integer()) return {true, r.integer()};
        return {false, 0};
    }
    bool SAdd(const std::string& key, const std::string& member) {
        auto r = Command("SADD %s %s", key.c_str(), member.c_str());
        return !(r.is_null() || r.is_err());
    }
    std::pair<bool, int> SRem(const std::string& key, const std::string& member) {
        auto r = Command("SREM %s %s", key.c_str(), member.c_str());
        if (r.is_null() || !r.is_integer()) {
            return {false, 0};
        }
        return {true, r.integer()};
    }
    std::pair<bool, std::vector<std::string>> SMembers(const std::string& key) {
        auto r = Command("SMEMBERS %s", key.c_str());
        if (r.is_nil()) return {true, std::vector<std::string>{}};
        if (r.is_array()) return {true, r.array()};
        return {false, std::vector<std::string>{}};
    }
    bool Subscribe(const std::string& channel) {
        // auto r = Command("SUBSCRIBE %s", channel.c_str());
        // return !(r.is_null() || r.is_err());
        return AppendCommand("SUBSCRIBE %s", channel.c_str());
    }
    std::pair<bool, int> Publish(const std::string& channel, const std::string& message) {
        auto r = Command("PUBLISH %s %s", channel.c_str(), message.c_str());
        if (r.is_null() || !r.is_integer()) {
            return {false, 0};
        }
        return {true, r.integer()};
    }
};