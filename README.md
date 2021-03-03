# rediscc
Golang Style C++ Redis Client

Unified interface for Single Redis client and Redis Cluster

Quickstart: 


    RedisOptions opts {
        IP : "127.0.0.1",
        Port : 6379,
        Type : REDIS_SINGLE,
        UsePool : false,
        PoolSize : 3,
    };
    auto rdb = RedisClient::NewClient(opts);
    if (rdb == nullptr) {
        cerr << "failed to create redis client" << endl;
        return -1;  
    }

    if (rdb->Set("key", "value") == false) {
        cerr << "SET failed" << endl; 
        return -1; 
    }

    if (auto [ok, val]  = rdb->Get("key"); !ok) {
        cerr << "GET failed" << endl;
        return -1;  
    }  else {
        if (val.has_value()) {
            cout << "key" << " = " << val.value() << endl; 
        } else {
            cout << "key is nil" << endl; 
        }
    }
