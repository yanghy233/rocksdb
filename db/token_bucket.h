#ifndef ROCKSDB_TOKEN_BUCKET_H
#define ROCKSDB_TOKEN_BUCKET_H

#include <cstdint>
#include <iostream>
#include <queue>
#include <sys/time.h>
#include "util/mutexlock.h"
#include "monitoring/statistics.h"
#include "options/cf_options.h"

#define DEFAULT_RATE (1024 * 1024 * 400)
#define DEFAULT_MIN_RATE (1024 * 1024 * 80)
#define DEFAULT_REFILL_PERIOD_US (40 * 1000) // default refill time
#define TUNE_REQUESTS (200 * 1000)

namespace ROCKSDB_NAMESPACE {

    class ColumnFamilyData;
    class VersionStorageInfo;

    class TokenBucket {
    public:
        explicit TokenBucket(ColumnFamilyData *cfd) : cfd_(cfd) {
            refill_period_us_ = DEFAULT_REFILL_PERIOD_US;
            max_rate_bytes_per_sec_ = 0x7fffffffffffffff;
        };

        ~TokenBucket() {
            stop_.store(true);
        }

        void Start() {
            start_time_ = NowTime();
            tune_time_ = NowTime();
            next_refill_time_ = NowTime() + refill_period_us_;
            new_rate_bytes_per_sec_ = DEFAULT_RATE;
            CalculateRefillBytesPerPeriod();
        }

        static int64_t NowTime() {
            struct timeval tv{};
            gettimeofday(&tv, nullptr);
            return static_cast<int64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
        }

        // 计算每个周期的补充字节数 ( default : 每100ms向令牌桶填充的数量 )
        void CalculateRefillBytesPerPeriod() {
            const int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();
            if (kMaxInt64 / new_rate_bytes_per_sec_ < refill_period_us_) {
                // Avoid unexpected result in the overflow case. The result now is still
                // inaccurate but is a number that is large enough.
                refill_bytes_per_period_ = kMaxInt64 / 1000000;
            } else {
                refill_bytes_per_period_ = std::max((int64_t) 100,
                                                    new_rate_bytes_per_sec_ * refill_period_us_ / 1000000);
            }
        }

        // 请求令牌
        void Request(int64_t bytes);

        void AdjustRate(int64_t last_rate);

        void CalculateMaxRate();

    private:
        ColumnFamilyData *cfd_;

        const rocksdb::ImmutableCFOptions* immutable_options_{nullptr};

        std::atomic<int64_t> max_rate_bytes_per_sec_;

        // 开始到现在的总请求数
        std::atomic<int64_t> total_requests_{0};

        // 开始到现在的总插入字节数
        std::atomic<int64_t> total_bytes_through_{0};

        // 开始到上一时刻的总插入字节数
        std::atomic<int64_t> last_bytes_through_{0};

        // 令牌桶剩余字节数
        std::atomic<int64_t> available_bytes_{0};

        // 新的速率
        std::atomic<int64_t> new_rate_bytes_per_sec_;

        // 重新填充令牌桶的周期 ( default = 100ms )
        std::atomic<int64_t> refill_period_us_;

        // 每隔 default = 100ms 向令牌桶填充的字节数
        std::atomic<int64_t> refill_bytes_per_period_;

        std::atomic<int64_t> start_time_;

        // 上次调整速率的时间
        std::atomic<int64_t> tune_time_;

        // 下次填充令牌桶的时间
        std::atomic<int64_t> next_refill_time_;

        // 互斥锁，用于保护对令牌桶状态的访问
        mutable port::Mutex m1_;  // 速率调整部分的锁
        mutable port::Mutex m2_;  // 令牌桶的获取部分的锁

        std::atomic<bool> stop_{false};

        std::atomic<bool> first_time_{true};

        std::atomic<uint64_t> last_flush_bytes_;
        std::atomic<uint64_t> last_compaction_bytes_;


    };
}  // namespace ROCKSDB_NAMESPACE

#endif  // ROCKSDB_TOKEN_BUCKET_H
