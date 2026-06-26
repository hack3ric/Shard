#include <cassert>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>

#include "util/json_config.h"
#include "smart/common.h"
#include "smart/thread.h"
#include "shard.h"

namespace {
class PthBmTarget {
public:
    explicit PthBmTarget(JsonConfig config) {
        int qp_count = sds::kMaxThreads;
        if (const char *env = getenv("PTH_BM_SHARD_QP_COUNT")) {
            int parsed = atoi(env);
            if (parsed > 0 && parsed <= static_cast<int>(sds::kMaxThreads)) {
                qp_count = parsed;
            }
        }

        multi_ = config.get("memory_servers").size() > 1;
        if (multi_) {
            multi_shard_.reset(new RemoteShardMultiShard(config, qp_count));
            int rc = multi_shard_->set_total_tasks(0);
            assert(!rc);
        } else {
            shard_.reset(new RemoteShard(config, qp_count));
            int rc = shard_->set_total_tasks(0);
            assert(!rc);
        }
    }

    void init_thread() {
        std::lock_guard<std::mutex> lock(init_mu_);
        if (multi_) {
            int rc = multi_shard_->register_current_thread();
            assert(!rc);
        } else {
            int rc = shard_->register_current_thread();
            assert(!rc);
        }
    }

    void read(int key) {
        std::string value;
        int rc = index().search(make_key(key), value);
        assert(rc == 0 || rc == ENOENT);
    }

    void insert(int key) {
        int rc = index().insert(make_key(key), make_value(0xdeadbeefULL));
        assert(rc == 0 || rc == EEXIST);
    }

    void update(int key) {
        int rc = index().update(make_key(key), make_value(0xdeadcafeULL));
        assert(rc == 0 || rc == ENOENT);
    }

    void remove(int key) {
        int rc = index().remove(make_key(key));
        assert(rc == 0 || rc == ENOENT);
    }

private:
    class IndexRef {
    public:
        explicit IndexRef(RemoteShard *single) : single_(single), multi_(nullptr) {}

        explicit IndexRef(RemoteShardMultiShard *multi) : single_(nullptr), multi_(multi) {}

        int search(const std::string &key, std::string &value) {
            return single_ ? single_->search(key, value) : multi_->search(key, value);
        }

        int insert(const std::string &key, const std::string &value) {
            return single_ ? single_->insert(key, value) : multi_->insert(key, value);
        }

        int update(const std::string &key, const std::string &value) {
            return single_ ? single_->update(key, value) : multi_->update(key, value);
        }

        int remove(const std::string &key) {
            return single_ ? single_->remove(key) : multi_->remove(key);
        }

    private:
        RemoteShard *single_;
        RemoteShardMultiShard *multi_;
    };

    IndexRef index() {
        return multi_ ? IndexRef(multi_shard_.get()) : IndexRef(shard_.get());
    }

    static std::string make_key(int key) {
        return std::string(reinterpret_cast<const char *>(&key), sizeof(key));
    }

    static std::string make_value(uint64_t value) {
        return std::string(reinterpret_cast<const char *>(&value), sizeof(value));
    }

    std::mutex init_mu_;
    bool multi_ = false;
    std::unique_ptr<RemoteShard> shard_;
    std::unique_ptr<RemoteShardMultiShard> multi_shard_;
};

thread_local std::unordered_set<PthBmTarget *> g_registered_targets;
}

extern "C" {
void *pth_bm_target_create() {
    const char *path = ROOT_DIR "/config/datastructure.json";
    if (getenv("APP_CONFIG_PATH")) {
        path = getenv("APP_CONFIG_PATH");
    }
    JsonConfig config = JsonConfig::load_file(path);
    return new PthBmTarget(config);
}

void pth_bm_target_init_thread(void *target) {
    auto *wrapper = static_cast<PthBmTarget *>(target);
    assert(wrapper);
    sds::GetThreadID();
    if (g_registered_targets.insert(wrapper).second) {
        wrapper->init_thread();
    }
}

void pth_bm_target_print_stat(void *target) {
    (void) target;
}

void pth_bm_target_destroy(void *target) {
    auto *wrapper = static_cast<PthBmTarget *>(target);
    delete wrapper;
    g_registered_targets.erase(wrapper);
}

void pth_bm_target_read(void *target, int key) {
    static_cast<PthBmTarget *>(target)->read(key);
}

void pth_bm_target_insert(void *target, int key) {
    static_cast<PthBmTarget *>(target)->insert(key);
}

void pth_bm_target_update(void *target, int key) {
    static_cast<PthBmTarget *>(target)->update(key);
}

void pth_bm_target_delete(void *target, int key) {
    static_cast<PthBmTarget *>(target)->remove(key);
}
}
