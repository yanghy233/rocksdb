#include <algorithm>
#include "db/token_bucket.h"
#include "version_set.h"

namespace ROCKSDB_NAMESPACE {
    void TokenBucket::Request(int64_t bytes) {
        if (stop_.load()) {
            return;
        }

        total_requests_.fetch_add(1);

        if (first_time_.load()) {
            first_time_.store(false);
            tune_time_.store(NowTime());
            total_bytes_through_.store(0);
            last_bytes_through_.store(0);
        }

        if (total_requests_.load() > 0 && total_requests_.load() % TUNE_REQUESTS == 0) {
//            MutexLock mu(&m1_);
            // 上一次 20w请求的真实速率 B/s
            int64_t last_bytes = total_bytes_through_.load() - last_bytes_through_.load();
            int64_t last_rate = last_bytes / ((NowTime() - tune_time_.load()) / 1000000.0);

            // 速率调整
            VersionStorageInfo *vsinfo = cfd_->current()->storage_info();
            new_rate_bytes_per_sec_.store((last_rate * 48) / (40 + vsinfo->NumLevelFiles(0)));
            std::cout << "[OurDB] last rate = " << last_rate << ", new rate = " << new_rate_bytes_per_sec_
                      << ", L0 file num = " << vsinfo->NumLevelFiles(0) << std::endl;

            tune_time_.store(NowTime());
            last_bytes_through_.store(total_bytes_through_.load());
            CalculateRefillBytesPerPeriod();
        }

        // 令牌的获取

        // 填充令牌或者等待
        {
            // MutexLock mu(&m2_);
            while (available_bytes_.load() < bytes) {
                if (NowTime() >= next_refill_time_.load()) {
                    next_refill_time_.store(NowTime() + refill_period_us_);
                    available_bytes_.fetch_add(refill_bytes_per_period_);
                } else {
                    MutexLock mu(&m2_);
                    if (NowTime() >= next_refill_time_.load()) {
                        continue;
                    }
                    port::CondVar cv(&m2_);
                    cv.TimedWait(next_refill_time_.load());
                }
            }
        }

        // 消费令牌
        available_bytes_.fetch_sub(bytes);
        total_bytes_through_.fetch_add(bytes);
    }
}
