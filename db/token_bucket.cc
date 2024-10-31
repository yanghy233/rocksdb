#include <algorithm>
#include "db/token_bucket.h"
#include "version_set.h"

namespace ROCKSDB_NAMESPACE {
    void TokenBucket::Request(int64_t bytes) {
        MutexLock mu(&m1_);
        if (stop_) {
            return;
        }

        total_requests_++;

        if (first_time_) {
            first_time_ = false;
            tune_time_ = NowTime();
            total_bytes_through_ = 0;
            last_bytes_through_ = 0;
        }

        if (total_requests_ > 0 && total_requests_ % TUNE_REQUESTS == 0) {
            // 上一次 20w请求的真实速率 B/s
            int64_t last_bytes = total_bytes_through_ - last_bytes_through_;
            int64_t last_rate = last_bytes / ((NowTime() - tune_time_) / 1000000.0);

            // 速率调整
            VersionStorageInfo *vsinfo = cfd_->current()->storage_info();
            new_rate_bytes_per_sec_ = (last_rate * 48) / (40 + vsinfo->NumLevelFiles(0));
            std::cout << "[OurDB] last rate = " << last_rate << ", new rate = " << new_rate_bytes_per_sec_
                      << ", L0 file num = " << vsinfo->NumLevelFiles(0) << std::endl;

            tune_time_ = NowTime();
            last_bytes_through_ = total_bytes_through_;
            CalculateRefillBytesPerPeriod();
        }

        // 令牌的获取

        // 填充令牌或者等待
        while (available_bytes_ < bytes) {
            if (NowTime() >= next_refill_time_) {
                next_refill_time_ = NowTime() + refill_period_us_;
//                if (available_bytes_ < refill_bytes_per_period_) {
                available_bytes_ += refill_bytes_per_period_;
//                }
            } else {
                port::CondVar cv(&m1_);
                cv.TimedWait(next_refill_time_);
            }
        }

        available_bytes_ -= bytes;
        total_bytes_through_ += bytes;
    }
}
