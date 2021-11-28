#pragma once
extern "C" {
#include <hiredis/hiredis.h>
#include <hiredis_cluster/hircluster.h>
}
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>
// Redis types
enum {
  REDIS_TYPE_SINGLE,
  REDIS_TYPE_CLUSTER,
};

template <typename T>
class ConnPool {
 public:
  ~ConnPool() {}
  ConnPool() {}
  void set_timeout(int timeout) { timeout_ = timeout * 1000000; }
  void put(T& c) { send(c); }
  std::pair<bool, T> get() {
    if (timeout_ < 0) return {true, recv()};
    auto c = recv(timeout_);
    if (!c.has_value()) return {false, nullptr};
    return {true, c.value()};
  }
  void send(T& item) {
    {
      std::lock_guard<std::mutex> lock(lk_);
      conns_.push(item);
    }
    cv_.notify_one();
  }
  T recv() {
    std::unique_lock<std::mutex> lock(lk_);
    cv_.wait(lock, [this] { return !conns_.empty(); });
    auto res = std::move(conns_.front());
    conns_.pop();
    return res;
  }
  std::optional<T> recv(int timeout_in_us) {
    std::unique_lock<std::mutex> lock(lk_);
    if (cv_.wait_for(lock, std::chrono::microseconds(timeout_in_us),
                     [this] { return !conns_.empty(); })) {
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

class RedisReply {
 public:
  RedisReply() {}
  RedisReply(redisReply* r) {
    auto deleter = [](auto p) {
      freeReplyObject(p); /*cout << "freed" << endl;*/
    };
    ptr_ = std::shared_ptr<redisReply>(r, deleter);
  }
  int type() const { return ptr_->type; }
  std::string typestr() const { return typestrs_.at(type()); }
  bool is_null() { return ptr_ == nullptr; }
  bool is_nil() { return ptr_ != nullptr && type() == REDIS_REPLY_NIL; }
  bool is_err() { return ptr_ != nullptr && type() == REDIS_REPLY_ERROR; }
  bool is_integer() { return ptr_ != nullptr && type() == REDIS_REPLY_INTEGER; }
  bool is_array() { return ptr_ != nullptr && type() == REDIS_REPLY_ARRAY; }
  bool is_str() { return ptr_ != nullptr && type() == REDIS_REPLY_STRING; }
  bool is_status() { return ptr_ != nullptr && type() == REDIS_REPLY_STATUS; }
  long long integer() const { return ptr_->integer; }
  std::string str() const { return std::string(ptr_->str, ptr_->len); }
  std::vector<std::string> array() const {
    std::vector<std::string> strs;
    for (size_t i = 0; i < ptr_->elements; i++) {
      if (ptr_->element[i]->str != nullptr)
        strs.push_back(ptr_->element[i]->str);
    }
    return strs;
  }
  std::unordered_map<std::string, std::string> map() const {
    std::unordered_map<std::string, std::string> m;
    std::vector<std::string> strs;
    for (size_t i = 0; i < ptr_->elements; i++) {
      strs.push_back(ptr_->element[i]->str);
      if (i % 2 == 1) m[strs[i - 1]] = strs[i];
    }
    return m;
  }
  redisReply* ptr() { return ptr_.get(); }

  // private:
  std::shared_ptr<redisReply> ptr_ = nullptr;
  std::unordered_map<int, std::string> typestrs_ = {
      {1, "REDIS_REPLY_STRING"},  {2, "REDIS_REPLY_ARRAY"},
      {3, "REDIS_REPLY_INTEGER"}, {4, "REDIS_REPLY_NIL"},
      {5, "REDIS_REPLY_STATUS"},  {6, "REDIS_REPLY_ERROR"}};
};

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
  bool HSet(const std::string& key, const std::string& field,
            const std::string& val) {
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
  std::pair<bool, std::optional<std::string>> HGet(const std::string& key,
                                                   const std::string& field) {
    auto r = Command("HGET %s %s", key.c_str(), field.c_str());
    if (r.is_nil()) return {true, std::nullopt};
    if (r.is_str()) return {true, r.str()};
    return {false, std::nullopt};
  }
  std::pair<bool, std::unordered_map<std::string, std::string>> HGetAll(
      const std::string& key) {
    auto r = Command("HGETALL %s", key.c_str());
    if (r.is_nil())
      return {true, std::unordered_map<std::string, std::string>{}};
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
  std::pair<bool, int> Publish(const std::string& channel,
                               const std::string& message) {
    auto r = Command("PUBLISH %s %s", channel.c_str(), message.c_str());
    if (r.is_null() || !r.is_integer()) {
      return {false, 0};
    }
    return {true, r.integer()};
  }
};

struct RedisOptions {
  std::string IP = "127.0.0.1";
  int Port = 6379;
  int Type = REDIS_TYPE_SINGLE;
  std::string Password;
  int DialTimeout = 10;  // second
  int MaxRetries = 3;    // Default is 3 retries for commands

  bool UsePool = false;
  int PoolSize = 3;
  int PoolTimeout = 10;  // second
  int Flags = 0;  // HIRCLUSTER_FLAG_ROUTE_USE_SLOTS; // only works for cluster
};

class RedisConnections {
 public:
  static std::shared_ptr<redisContext> NewConn(const RedisOptions& opt) {
    struct timeval tv;
    tv.tv_sec = opt.DialTimeout;
    tv.tv_usec = 0;
    bool KeepAlive = true;

    auto deleter = [](auto c) {
      redisFree(c); /*std::cout << "redis context released" << std::endl; */
    };
    auto c = std::shared_ptr<redisContext>(
        redisConnectWithTimeout(opt.IP.c_str(), opt.Port, tv), deleter);
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
      auto reply =
          (redisReply*)redisCommand(c.get(), "AUTH %s", opt.Password.c_str());
      if (reply == nullptr || reply->type == REDIS_REPLY_ERROR) {
        return nullptr;
      }
    }
    return c;
  }
  static std::shared_ptr<redisClusterContext> NewClusterConn(
      const RedisOptions& opt) {
    struct timeval tv;
    tv.tv_sec = opt.DialTimeout;
    tv.tv_usec = 0;
    std::string addrs = opt.IP + ":" + std::to_string(opt.Port);
    auto cc = std::shared_ptr<redisClusterContext>(
        redisClusterConnectWithTimeout(addrs.c_str(), tv, opt.Flags),
        [](auto c) { redisClusterFree(c); });
    if (cc == nullptr || cc->err != 0) {
      if (cc == nullptr) {
        std::cerr << "Redis: null context" << std::endl;
      } else {
        std::cerr << "Redis: " << cc->errstr << std::endl;
      }
      return nullptr;
    }
    // if (redisClusterSetOptionTimeout(cc.get(), { 0, opt.TimeOut }) !=
    // REDIS_OK) {
    //     return nullptr;
    // }
    return cc;
  }
};

class RedisClient : public RedisCmdable {
 public:
  bool Connect(const RedisOptions& opts) {
    return opts.UsePool ? connectWithPool(opts) : connectWithoutPool(opts);
  }
  bool is_null() const { return c_ == nullptr; }
  int err() const { return c_->err; }
  void reset_err() { c_->err = 0; }
  std::string errstr() const { return c_->errstr; }
  redisContext* Context() { return c_.get(); }
  static std::shared_ptr<RedisClient> NewClient(const RedisOptions& opts) {
    std::shared_ptr<RedisClient> client(new RedisClient);
    if (client->Connect(opts)) return client;
    return nullptr;
  }
  bool SetTimeout(const struct timeval& tv) {
    return redisSetTimeout(c_.get(), tv) == REDIS_OK;
  }
  std::string GetSrvTag() const {
    return opts_.IP + ":" + std::to_string(opts_.Port) + ":" +
           std::to_string(opts_.Type);
  }

 public:  // Generic APIs
  // run a redis command with retry
  RedisReply Command(const char* format, ...) {
    RedisReply r;
    if (!opts_.UsePool) {
      va_list ap;
      va_start(ap, format);
      r = (redisReply*)redisvCommand(c_.get(), format, ap);
      va_end(ap);
      // if redis is disconnected for some reason
      int i = 0;
      while (r.is_null() && i++ < opts_.MaxRetries) {
        std::cerr << "Redis disconnected: reconencting and retrying ... "
                  << std::endl;
        usleep(8000);  // backoff 8 milliseconds
        c_ = RedisConnections::NewConn(opts_);
        if (c_ != nullptr) {
          va_list ap;
          va_start(ap, format);
          r = (redisReply*)redisvCommand(c_.get(), format, ap);
          va_end(ap);
        }
      }
    } else {
      auto [ok, c] = pool_.get();
      if (!ok) {
        std::cerr << "failed to fetch conn from Redis pool: timeout"
                  << std::endl;
        return r;
      }
      va_list ap;
      va_start(ap, format);
      r = (redisReply*)redisvCommand(c.get(), format, ap);
      va_end(ap);

      // if redis is disconnected for some reason
      int i = 0;
      while (r.is_null() && i++ < opts_.MaxRetries) {
        std::cerr << "Redis disconnected: reconencting and retrying ... "
                  << std::endl;
        usleep(8000);  // backoff 8 milliseconds
        c = RedisConnections::NewConn(opts_);
        if (c != nullptr) {
          va_list ap;
          va_start(ap, format);
          r = (redisReply*)redisvCommand(c.get(), format, ap);
          va_end(ap);
        }
      }
      pool_.put(c);
    }
    return r;
  }
  bool AppendCommand(const char* format, ...) {
    va_list ap;
    va_start(ap, format);
    auto retc = redisvAppendCommand(c_.get(), format, ap);
    va_end(ap);
    return retc == REDIS_OK;
  }
  // only works for connection without pooling
  std::pair<bool, RedisReply> GetReply() {
    if (opts_.UsePool) {
      std::cerr << "command only available for client without connection pool"
                << std::endl;
      return {false, RedisReply{}};
    }
    // assert(opts_.UsePool == false);
    void* reply = nullptr;
    int retc = redisGetReply(c_.get(), &reply);
    RedisReply r = (redisReply*)reply;
    return {retc == REDIS_OK, r};
  }

 private:
  std::shared_ptr<redisContext> c_;
  RedisOptions opts_;
  ConnPool<std::shared_ptr<redisContext>> pool_;
  bool connectWithoutPool(const RedisOptions& opts) {
    opts_ = opts;
    c_ = RedisConnections::NewConn(opts);
    return c_ != nullptr;
  }
  bool connectWithPool(const RedisOptions& opts) {
    opts_ = opts;
    for (int i = 0; i < opts.PoolSize; i++) {
      auto c = RedisConnections::NewConn(opts);
      if (c == nullptr) {
        std::cerr << "failed to creat new connection" << std::endl;
        return false;
      }
      pool_.put(c);
    }
    return true;
  }
};

class ClusterClient : public RedisCmdable {
 public:
  bool Connect(const RedisOptions& opts) {
    return opts.UsePool ? connectWithPool(opts) : connectWithoutPool(opts);
  }
  bool is_null() const { return c_ == nullptr; }
  int err() const { return c_->err; }
  void reset_err() {
    // in order to use pub/sub in cluster mode, timeout error must be reset,
    // however, hiredis_cluster has not implement this function yet
    // redisClusterResetErrors(c_.get());
    // c_->err = 0;
  }
  std::string errstr() const { return c_->errstr; }
  redisClusterContext* Context() { return c_.get(); }
  static std::shared_ptr<ClusterClient> NewClient(const RedisOptions& opts) {
    std::shared_ptr<ClusterClient> client(new ClusterClient);
    if (client->Connect(opts)) return client;
    return nullptr;
  }
  bool SetTimeout(const struct timeval& tv) {
    return redisClusterSetOptionTimeout(c_.get(), tv) == REDIS_OK;
  }
  std::string GetSrvTag() const {
    return opts_.IP + ":" + std::to_string(opts_.Port) + ":" +
           std::to_string(opts_.Type);
  }

 public:  // Generic APIs
  // run a redis command with retry
  RedisReply Command(const char* format, ...) {
    RedisReply r;
    if (!opts_.UsePool) {
      va_list ap;
      va_start(ap, format);
      r = (redisReply*)redisClustervCommand(c_.get(), format, ap);
      va_end(ap);
      // if redis is disconnected for some reason
      int i = 0;
      while (r.is_null() && i++ < opts_.MaxRetries) {
        std::cerr << "Redis disconnected: reconencting and retrying ... "
                  << std::endl;
        usleep(8000);  // backoff 8 milliseconds
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
        std::cerr << "failed to fetch conn from Redis pool: timeout"
                  << std::endl;
        return r;
      }
      va_list ap;
      va_start(ap, format);
      r = (redisReply*)redisClustervCommand(c.get(), format, ap);
      va_end(ap);

      // if redis is disconnected for some reason
      int i = 0;
      while (r.is_null() && i++ < opts_.MaxRetries) {
        std::cerr << "Redis disconnected: reconencting and retrying ... "
                  << std::endl;
        usleep(8000);  // backoff 8 milliseconds
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
  bool AppendCommand(const char* format, ...) {
    va_list ap;
    va_start(ap, format);
    auto retc = redisClustervAppendCommand(c_.get(), format, ap);
    va_end(ap);
    return retc == REDIS_OK;
  }
  // only works for connection without pooling
  std::pair<bool, RedisReply> GetReply() {
    if (opts_.UsePool) {
      std::cerr << "command only available for client without connection pool"
                << std::endl;
      return {false, RedisReply{}};
    }
    void* reply = nullptr;
    int retc = redisClusterGetReply(c_.get(), &reply);
    RedisReply r = (redisReply*)reply;
    return {retc == REDIS_OK, r};
  }

 private:
  std::shared_ptr<redisClusterContext> c_;
  RedisOptions opts_;
  ConnPool<std::shared_ptr<redisClusterContext>> pool_;
  bool connectWithoutPool(const RedisOptions& opts) {
    opts_ = opts;
    c_ = RedisConnections::NewClusterConn(opts);
    return c_ != nullptr;
  }
  bool connectWithPool(const RedisOptions& opts) {
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
