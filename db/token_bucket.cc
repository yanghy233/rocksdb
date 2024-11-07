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
            if (immutable_options_ == nullptr) {
                immutable_options_ = cfd_->ioptions();
            }
            first_time_.store(false);
            tune_time_.store(NowTime());
            total_bytes_through_.store(0);
            last_bytes_through_.store(0);
            if (immutable_options_->statistics) {
                last_flush_bytes_ = immutable_options_->statistics->getTickerCount(FLUSH_WRITE_BYTES);
                last_compaction_bytes_ =
                        immutable_options_->statistics->getTickerCount(COMPACT_WRITE_BYTES) +
                        immutable_options_->statistics->getTickerCount(COMPACT_READ_BYTES);
            }
        }

        if (total_requests_.load() > 0 && total_requests_.load() % TUNE_REQUESTS == 0) {
            // the actual rate of last time
            int64_t last_bytes = total_bytes_through_.load() - last_bytes_through_.load();
            int64_t last_rate = last_bytes / ((NowTime() - tune_time_.load()) / 1000000.0);

            // Adjust the rate
            AdjustRate(last_rate);
        }

        // fill the token bucket or wait for enough tokens
        {
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

        // consume tokens
        available_bytes_.fetch_sub(bytes);
        total_bytes_through_.fetch_add(bytes);
    }

    void TokenBucket::AdjustRate(int64_t last_rate) {
        const ColumnFamilyOptions &cf_options = cfd_->GetLatestCFOptions();
        int max_write_buffer_number = cf_options.max_write_buffer_number;
        int min_write_buffer_number_to_merge = cf_options.min_write_buffer_number_to_merge;
        int level0_slowdown_writes_trigger = cf_options.level0_slowdown_writes_trigger;
        int level0_file_num_compaction_trigger = cf_options.level0_file_num_compaction_trigger;
        int current_write_buffer = cfd_->imm()->NumNotFlushed();
        VersionStorageInfo *vsinfo = cfd_->current()->storage_info();
        int current_level0_file_num = vsinfo->NumLevelFiles(0);

        uint64_t flush_bytes_rate = 3, compaction_bytes_rate = 97;
        if (immutable_options_->statistics) {
            uint64_t current_flush_bytes = immutable_options_->statistics->getTickerCount(FLUSH_WRITE_BYTES);
            uint64_t current_compaction_bytes =
                    immutable_options_->statistics->getTickerCount(COMPACT_WRITE_BYTES) +
                    immutable_options_->statistics->getTickerCount(COMPACT_READ_BYTES);
            flush_bytes_rate = current_flush_bytes - last_flush_bytes_;
            compaction_bytes_rate = current_compaction_bytes - last_compaction_bytes_;
            last_flush_bytes_ = current_flush_bytes;
            last_compaction_bytes_ = current_compaction_bytes;

//            std::cout << "current_flush_bytes = " << current_flush_bytes << ", last_flush_bytes_ = "
//                      << last_flush_bytes_ << std::endl;
//            std::cout << "current_compaction_bytes = " << current_compaction_bytes << ", last_compaction_bytes_ = "
//                      << last_compaction_bytes_ << std::endl;
        } else {
            last_flush_bytes_ = 3;
            last_compaction_bytes_ = 97;
        }


        CalculateMaxRate();

        int64_t mem_ratio = (last_rate * (min_write_buffer_number_to_merge + 2 * max_write_buffer_number)) /
                            (current_write_buffer + 2 * max_write_buffer_number);
        int64_t l0_ratio = (last_rate * (level0_file_num_compaction_trigger + 2 * level0_slowdown_writes_trigger + 4)) /
                           (current_level0_file_num + 2 * level0_slowdown_writes_trigger);

        double k1, k2;
        if (flush_bytes_rate + compaction_bytes_rate == 0) {
            k1 = 0.5;
            k2 = 0.5;
        } else {
            auto flush_bytes_rate_d = static_cast<double>(flush_bytes_rate);
            auto compaction_bytes_rate_d = static_cast<double>(compaction_bytes_rate);
            k1 = flush_bytes_rate_d / (flush_bytes_rate_d + compaction_bytes_rate_d);
            k2 = 1 - k1;
        }

//        std::cout << "k1 = " << k1 << ", k2 = " << k2 << std::endl;
//        std::cout << "max_rate = " << max_rate_bytes_per_sec_ << std::endl;

        new_rate_bytes_per_sec_.store(k1 * mem_ratio + k2 * l0_ratio);
        if (new_rate_bytes_per_sec_ > max_rate_bytes_per_sec_) {
            new_rate_bytes_per_sec_.store(max_rate_bytes_per_sec_);
        }

//        new_rate_bytes_per_sec_.store((last_rate * 48) / (40 + vsinfo->NumLevelFiles(0)));
        std::cout << "[OurDB] last rate = " << last_rate << ", new rate = " << new_rate_bytes_per_sec_
                  << ", ratio = " << (k1 * mem_ratio + k2 * l0_ratio) / last_rate
                  << ", k1 = " << k1 << ", k2 = " << k2
                  << ", L0 file num = " << vsinfo->NumLevelFiles(0) << std::endl;

        tune_time_.store(NowTime());
        last_bytes_through_.store(total_bytes_through_.load());
        CalculateRefillBytesPerPeriod();
    }

    void TokenBucket::CalculateMaxRate() {
        const int64_t kMaxInt64 = std::numeric_limits<int64_t>::max();
        const int64_t default_disk_rate = 1024 * 1024 * 700;
        if (last_flush_bytes_ + last_compaction_bytes_ == 0 || total_bytes_through_ > kMaxInt64 / default_disk_rate)
            max_rate_bytes_per_sec_ = default_disk_rate;
        else {
            max_rate_bytes_per_sec_ =
                    default_disk_rate * total_bytes_through_ / (last_flush_bytes_ + last_compaction_bytes_);
            if (max_rate_bytes_per_sec_ < DEFAULT_MIN_RATE) {
                max_rate_bytes_per_sec_ = DEFAULT_MIN_RATE;
            }
        }
    }
}
