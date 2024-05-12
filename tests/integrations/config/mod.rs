// Copyright 2020 WHTCORPS INC Project Authors. Licensed Under Apache-2.0

use std::fs::File;
use std::io::Read;
use std::path::PathBuf;

use slog::Level;

use batch_system::Config as BatchSystemConfig;
use encryption::{EncryptionConfig, FileConfig, MasterKeyConfig};
use engine_lmdb::config::{BlobRunMode, CompressionType, LogLevel, PerfLevel};
use engine_lmdb::raw::{
    CompactionPriority, DBCompactionStyle, DBCompressionType, DBRateLimiterMode, DBRecoveryMode,
};
use ekvproto::encryption_timeshare::EncryptionMethod;
use fidel_client::Config as FidelConfig;
use violetabftstore::interlock::{Config as CopConfig, ConsistencyCheckMethod};
use violetabftstore::store::Config as VioletaBftstoreConfig;
use security::SecurityConfig;
use edb::config::*;
use edb::import::Config as ImportConfig;
use edb::server::config::GrpcCompressionType;
use edb::server::gc_worker::GcConfig;
use edb::server::lock_manager::Config as PessimisticTxnConfig;
use edb::server::Config as ServerConfig;
use edb::causet_storage::config::{BlockCacheConfig, Config as StorageConfig};
use violetabftstore::interlock::::collections::HashSet;
use violetabftstore::interlock::::config::{LogFormat, OptionReadableSize, ReadableDuration, ReadableSize};

mod dynamic;
mod test_config_client;

#[test]
fn test_toml_serde() {
    let value = EINSTEINDBConfig::default();
    let dump = toml::to_string_pretty(&value).unwrap();
    let load = toml::from_str(&dump).unwrap();
    assert_eq!(value, load);
}

// Read a file in project directory. It is similar to `include_str!`,
// but `include_str!` a large string literal increases compile time.
// See more: https://github.com/rust-lang/rust/issues/39352
fn read_file_in_project_dir(path: &str) -> String {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.push(path);
    let mut f = File::open(p).unwrap();
    let mut buffer = String::new();
    f.read_to_string(&mut buffer).unwrap();
    buffer
}

#[test]
fn test_serde_custom_edb_config() {
    let mut value = EINSTEINDBConfig::default();
    value.log_level = Level::Debug;
    value.log_file = "foo".to_owned();
    value.log_format = LogFormat::Json;
    value.slow_log_file = "slow_foo".to_owned();
    value.slow_log_memory_barrier = ReadableDuration::secs(1);
    value.server = ServerConfig {
        cluster_id: 0, // KEEP IT ZERO, it is skipped by serde.
        addr: "example.com:443".to_owned(),
        labels: map! { "a".to_owned() => "b".to_owned() },
        advertise_addr: "example.com:443".to_owned(),
        status_addr: "example.com:443".to_owned(),
        advertise_status_addr: "example.com:443".to_owned(),
        status_thread_pool_size: 1,
        max_grpc_lightlike_msg_len: 6 * (1 << 20),
        concurrent_lightlike_snap_limit: 4,
        concurrent_recv_snap_limit: 4,
        grpc_compression_type: GrpcCompressionType::Gzip,
        grpc_concurrency: 123,
        grpc_concurrent_stream: 1_234,
        grpc_memory_pool_quota: ReadableSize(123_456),
        grpc_violetabft_conn_num: 123,
        grpc_stream_initial_window_size: ReadableSize(12_345),
        grpc_keepalive_time: ReadableDuration::secs(3),
        grpc_keepalive_timeout: ReadableDuration::secs(60),
        lightlike_point_concurrency: None,
        lightlike_point_max_tasks: None,
        lightlike_point_stack_size: None,
        lightlike_point_recursion_limit: 100,
        lightlike_point_stream_channel_size: 16,
        lightlike_point_batch_row_limit: 64,
        lightlike_point_stream_batch_row_limit: 4096,
        lightlike_point_enable_batch_if_possible: true,
        lightlike_point_request_max_handle_duration: ReadableDuration::secs(12),
        lightlike_point_check_memory_locks: false,
        lightlike_point_max_concurrency: 10,
        snap_max_write_bytes_per_sec: ReadableSize::mb(10),
        snap_max_total_size: ReadableSize::gb(10),
        stats_concurrency: 10,
        heavy_load_memory_barrier: 1000,
        heavy_load_wait_duration: ReadableDuration::millis(2),
        enable_request_batch: false,
    };
    value.readpool = ReadPoolConfig {
        unified: UnifiedReadPoolConfig {
            min_thread_count: 5,
            max_thread_count: 10,
            stack_size: ReadableSize::mb(20),
            max_tasks_per_worker: 2200,
        },
        causet_storage: StorageReadPoolConfig {
            use_unified_pool: Some(true),
            high_concurrency: 1,
            normal_concurrency: 3,
            low_concurrency: 7,
            max_tasks_per_worker_high: 1000,
            max_tasks_per_worker_normal: 1500,
            max_tasks_per_worker_low: 2500,
            stack_size: ReadableSize::mb(20),
        },
        interlock: CoprReadPoolConfig {
            use_unified_pool: Some(false),
            high_concurrency: 2,
            normal_concurrency: 4,
            low_concurrency: 6,
            max_tasks_per_worker_high: 2000,
            max_tasks_per_worker_normal: 1000,
            max_tasks_per_worker_low: 3000,
            stack_size: ReadableSize::mb(12),
        },
    };
    value.metric = MetricConfig {
        interval: ReadableDuration::secs(12),
        address: "example.com:443".to_owned(),
        job: "edb_1".to_owned(),
    };
    let mut apply_batch_system = BatchSystemConfig::default();
    apply_batch_system.max_batch_size = 22;
    apply_batch_system.pool_size = 4;
    apply_batch_system.reschedule_duration = ReadableDuration::secs(3);
    let mut store_batch_system = BatchSystemConfig::default();
    store_batch_system.max_batch_size = 21;
    store_batch_system.pool_size = 3;
    store_batch_system.reschedule_duration = ReadableDuration::secs(2);
    value.violetabft_store = VioletaBftstoreConfig {
        prevote: false,
        violetabftdb_path: "/var".to_owned(),
        capacity: ReadableSize(123),
        violetabft_base_tick_interval: ReadableDuration::secs(12),
        violetabft_heartbeat_ticks: 1,
        violetabft_election_timeout_ticks: 12,
        violetabft_min_election_timeout_ticks: 14,
        violetabft_max_election_timeout_ticks: 20,
        violetabft_max_size_per_msg: ReadableSize::mb(12),
        violetabft_max_inflight_msgs: 123,
        violetabft_entry_max_size: ReadableSize::mb(12),
        violetabft_log_gc_tick_interval: ReadableDuration::secs(12),
        violetabft_log_gc_memory_barrier: 12,
        violetabft_log_gc_count_limit: 12,
        violetabft_log_gc_size_limit: ReadableSize::kb(1),
        violetabft_log_reserve_max_ticks: 100,
        violetabft_engine_purge_interval: ReadableDuration::minutes(20),
        violetabft_entry_cache_life_time: ReadableDuration::secs(12),
        violetabft_reject_transfer_leader_duration: ReadableDuration::secs(3),
        split_brane_check_tick_interval: ReadableDuration::secs(12),
        brane_split_check_diff: ReadableSize::mb(6),
        brane_compact_check_interval: ReadableDuration::secs(12),
        clean_stale_peer_delay: ReadableDuration::secs(0),
        brane_compact_check_step: 1_234,
        brane_compact_min_tombstones: 999,
        brane_compact_tombstones_percent: 33,
        fidel_heartbeat_tick_interval: ReadableDuration::minutes(12),
        fidel_store_heartbeat_tick_interval: ReadableDuration::secs(12),
        notify_capacity: 12_345,
        snap_mgr_gc_tick_interval: ReadableDuration::minutes(12),
        snap_gc_timeout: ReadableDuration::hours(12),
        messages_per_tick: 12_345,
        max_peer_down_duration: ReadableDuration::minutes(12),
        max_leader_missing_duration: ReadableDuration::hours(12),
        abnormal_leader_missing_duration: ReadableDuration::hours(6),
        peer_stale_state_check_interval: ReadableDuration::hours(2),
        leader_transfer_max_log_lag: 123,
        snap_apply_batch_size: ReadableSize::mb(12),
        lock_causet_compact_interval: ReadableDuration::minutes(12),
        lock_causet_compact_bytes_memory_barrier: ReadableSize::mb(123),
        consistency_check_interval: ReadableDuration::secs(12),
        report_brane_flow_interval: ReadableDuration::minutes(12),
        violetabft_store_max_leader_lease: ReadableDuration::secs(12),
        right_derive_when_split: false,
        allow_remove_leader: true,
        merge_max_log_gap: 3,
        merge_check_tick_interval: ReadableDuration::secs(11),
        use_delete_cone: true,
        cleanup_import_sst_interval: ReadableDuration::minutes(12),
        brane_max_size: ReadableSize(0),
        brane_split_size: ReadableSize(0),
        local_read_batch_size: 33,
        apply_batch_system,
        store_batch_system,
        future_poll_size: 2,
        hibernate_branes: false,
        hibernate_timeout: ReadableDuration::hours(1),
        early_apply: false,
        dev_assert: true,
        apply_yield_duration: ReadableDuration::millis(333),
        perf_level: PerfLevel::EnableTime,
    };
    value.fidel = FidelConfig::new(vec!["example.com:443".to_owned()]);
    let titan_causet_config = NoetherCfConfig {
        min_blob_size: ReadableSize(2018),
        blob_file_compression: CompressionType::Zstd,
        blob_cache_size: ReadableSize::gb(12),
        min_gc_batch_size: ReadableSize::kb(12),
        max_gc_batch_size: ReadableSize::mb(12),
        discardable_ratio: 0.00156,
        sample_ratio: 0.982,
        merge_small_file_memory_barrier: ReadableSize::kb(21),
        blob_run_mode: BlobRunMode::Fallback,
        level_merge: true,
        cone_merge: true,
        max_sorted_runs: 100,
        gc_merge_rewrite: true,
    };
    let titan_db_config = NoetherDBConfig {
        enabled: true,
        dirname: "bar".to_owned(),
        disable_gc: false,
        max_background_gc: 9,
        purge_obsolete_files_period: ReadableDuration::secs(1),
    };
    value.lmdb = DbConfig {
        wal_recovery_mode: DBRecoveryMode::AbsoluteConsistency,
        wal_dir: "/var".to_owned(),
        wal_ttl_seconds: 1,
        wal_size_limit: ReadableSize::kb(1),
        max_total_wal_size: ReadableSize::gb(1),
        max_background_jobs: 12,
        max_manifest_file_size: ReadableSize::mb(12),
        create_if_missing: false,
        max_open_files: 12_345,
        enable_statistics: false,
        stats_dump_period: ReadableDuration::minutes(12),
        compaction_readahead_size: ReadableSize::kb(1),
        info_log_max_size: ReadableSize::kb(1),
        info_log_roll_time: ReadableDuration::secs(12),
        info_log_keep_log_file_num: 1000,
        info_log_dir: "/var".to_owned(),
        info_log_level: LogLevel::Info,
        rate_bytes_per_sec: ReadableSize::kb(1),
        rate_limiter_refill_period: ReadableDuration::millis(10),
        rate_limiter_mode: DBRateLimiterMode::AllIo,
        auto_tuned: true,
        bytes_per_sync: ReadableSize::mb(1),
        wal_bytes_per_sync: ReadableSize::kb(32),
        max_sub_compactions: 12,
        wriBlock_file_max_buffer_size: ReadableSize::mb(12),
        use_direct_io_for_flush_and_compaction: true,
        enable_pipelined_write: false,
        enable_multi_batch_write: false,
        enable_unordered_write: true,
        defaultcauset: DefaultCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: false,
            titan: titan_causet_config.clone(),
            prop_size_index_distance: 4000000,
            prop_tuplespaceInstanton_index_distance: 40000,
            enable_doubly_skiplist: false,
        },
        writecauset: WriteCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: false,
            titan: NoetherCfConfig {
                min_blob_size: ReadableSize(1024), // default value
                blob_file_compression: CompressionType::Lz4,
                blob_cache_size: ReadableSize::mb(0),
                min_gc_batch_size: ReadableSize::mb(16),
                max_gc_batch_size: ReadableSize::mb(64),
                discardable_ratio: 0.5,
                sample_ratio: 0.1,
                merge_small_file_memory_barrier: ReadableSize::mb(8),
                blob_run_mode: BlobRunMode::ReadOnly,
                level_merge: false,
                cone_merge: true,
                max_sorted_runs: 20,
                gc_merge_rewrite: false,
            },
            prop_size_index_distance: 4000000,
            prop_tuplespaceInstanton_index_distance: 40000,
            enable_doubly_skiplist: true,
        },
        lockcauset: LockCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: false,
            titan: NoetherCfConfig {
                min_blob_size: ReadableSize(1024), // default value
                blob_file_compression: CompressionType::Lz4,
                blob_cache_size: ReadableSize::mb(0),
                min_gc_batch_size: ReadableSize::mb(16),
                max_gc_batch_size: ReadableSize::mb(64),
                discardable_ratio: 0.5,
                sample_ratio: 0.1,
                merge_small_file_memory_barrier: ReadableSize::mb(8),
                blob_run_mode: BlobRunMode::ReadOnly, // default value
                level_merge: false,
                cone_merge: true,
                max_sorted_runs: 20,
                gc_merge_rewrite: false,
            },
            prop_size_index_distance: 4000000,
            prop_tuplespaceInstanton_index_distance: 40000,
            enable_doubly_skiplist: true,
        },
        violetabftcauset: VioletaBftCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: false,
            titan: NoetherCfConfig {
                min_blob_size: ReadableSize(1024), // default value
                blob_file_compression: CompressionType::Lz4,
                blob_cache_size: ReadableSize::mb(0),
                min_gc_batch_size: ReadableSize::mb(16),
                max_gc_batch_size: ReadableSize::mb(64),
                discardable_ratio: 0.5,
                sample_ratio: 0.1,
                merge_small_file_memory_barrier: ReadableSize::mb(8),
                blob_run_mode: BlobRunMode::ReadOnly, // default value
                level_merge: false,
                cone_merge: true,
                max_sorted_runs: 20,
                gc_merge_rewrite: false,
            },
            prop_size_index_distance: 4000000,
            prop_tuplespaceInstanton_index_distance: 40000,
            enable_doubly_skiplist: true,
        },
        ver_defaultcauset: VersionCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: false,
            titan: titan_causet_config.clone(),
            prop_size_index_distance: 4000000,
            prop_tuplespaceInstanton_index_distance: 40000,
            enable_doubly_skiplist: false,
        },
        titan: titan_db_config.clone(),
    };
    value.violetabftdb = VioletaBftDbConfig {
        info_log_level: LogLevel::Info,
        wal_recovery_mode: DBRecoveryMode::SkipAnyCorruptedRecords,
        wal_dir: "/var".to_owned(),
        wal_ttl_seconds: 1,
        wal_size_limit: ReadableSize::kb(12),
        max_total_wal_size: ReadableSize::gb(1),
        max_background_jobs: 12,
        max_manifest_file_size: ReadableSize::mb(12),
        create_if_missing: false,
        max_open_files: 12_345,
        enable_statistics: false,
        stats_dump_period: ReadableDuration::minutes(12),
        compaction_readahead_size: ReadableSize::kb(1),
        info_log_max_size: ReadableSize::kb(1),
        info_log_roll_time: ReadableDuration::secs(1),
        info_log_keep_log_file_num: 1000,
        info_log_dir: "/var".to_owned(),
        max_sub_compactions: 12,
        wriBlock_file_max_buffer_size: ReadableSize::mb(12),
        use_direct_io_for_flush_and_compaction: true,
        enable_pipelined_write: false,
        enable_unordered_write: false,
        allow_concurrent_memBlock_write: false,
        bytes_per_sync: ReadableSize::mb(1),
        wal_bytes_per_sync: ReadableSize::kb(32),
        defaultcauset: VioletaBftDefaultCfConfig {
            block_size: ReadableSize::kb(12),
            block_cache_size: ReadableSize::gb(12),
            disable_block_cache: false,
            cache_index_and_filter_blocks: false,
            pin_l0_filter_and_index_blocks: false,
            use_bloom_filter: false,
            optimize_filters_for_hits: false,
            whole_key_filtering: true,
            bloom_filter_bits_per_key: 123,
            block_based_bloom_filter: true,
            read_amp_bytes_per_bit: 0,
            compression_per_level: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
                DBCompressionType::No,
                DBCompressionType::Zstd,
                DBCompressionType::Lz4,
            ],
            write_buffer_size: ReadableSize::mb(1),
            max_write_buffer_number: 12,
            min_write_buffer_number_to_merge: 12,
            max_bytes_for_level_base: ReadableSize::kb(12),
            target_file_size_base: ReadableSize::kb(123),
            level0_file_num_compaction_trigger: 123,
            level0_slowdown_writes_trigger: 123,
            level0_stop_writes_trigger: 123,
            max_compaction_bytes: ReadableSize::gb(1),
            compaction_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_level_bytes: true,
            num_levels: 4,
            max_bytes_for_level_multiplier: 8,
            compaction_style: DBCompactionStyle::Universal,
            disable_auto_compactions: true,
            soft_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            hard_plightlikeing_compaction_bytes_limit: ReadableSize::gb(12),
            force_consistency_checks: false,
            titan: titan_causet_config,
            prop_size_index_distance: 4000000,
            prop_tuplespaceInstanton_index_distance: 40000,
            enable_doubly_skiplist: true,
        },
        titan: titan_db_config,
    };
    value.violetabft_engine.enable = true;
    value.violetabft_engine.mut_config().dir = "test-dir".to_owned();
    value.causet_storage = StorageConfig {
        data_dir: "/var".to_owned(),
        gc_ratio_memory_barrier: 1.2,
        max_key_size: 8192,
        interlock_semaphore_concurrency: 123,
        interlock_semaphore_worker_pool_size: 1,
        interlock_semaphore_plightlikeing_write_memory_barrier: ReadableSize::kb(123),
        reserve_space: ReadableSize::gb(2),
        enable_async_commit: false,
        block_cache: BlockCacheConfig {
            shared: true,
            capacity: OptionReadableSize(Some(ReadableSize::gb(40))),
            num_shard_bits: 10,
            strict_capacity_limit: true,
            high_pri_pool_ratio: 0.8,
            memory_allocator: Some(String::from("nodump")),
        },
    };
    value.interlock = CopConfig {
        split_brane_on_Block: false,
        batch_split_limit: 1,
        brane_max_size: ReadableSize::mb(12),
        brane_split_size: ReadableSize::mb(12),
        brane_max_tuplespaceInstanton: 100000,
        brane_split_tuplespaceInstanton: 100000,
        consistency_check_method: ConsistencyCheckMethod::Mvcc,
    };
    let mut cert_allowed_cn = HashSet::default();
    cert_allowed_cn.insert("example.edb.com".to_owned());
    value.security = SecurityConfig {
        ca_path: "invalid path".to_owned(),
        cert_path: "invalid path".to_owned(),
        key_path: "invalid path".to_owned(),
        override_ssl_target: "".to_owned(),
        cert_allowed_cn,
        encryption: EncryptionConfig {
            data_encryption_method: EncryptionMethod::Aes128Ctr,
            data_key_rotation_period: ReadableDuration::days(14),
            master_key: MasterKeyConfig::File {
                config: FileConfig {
                    path: "/master/key/path".to_owned(),
                },
            },
            previous_master_key: MasterKeyConfig::Plaintext,
        },
    };
    value.backup = BackupConfig { num_threads: 456 };
    value.import = ImportConfig {
        num_threads: 123,
        stream_channel_window: 123,
        import_mode_timeout: ReadableDuration::secs(1453),
    };
    value.panic_when_unexpected_key_or_data = true;
    value.gc = GcConfig {
        ratio_memory_barrier: 1.2,
        batch_tuplespaceInstanton: 256,
        max_write_bytes_per_sec: ReadableSize::mb(10),
        enable_compaction_filter: true,
        compaction_filter_skip_version_check: true,
    };
    value.pessimistic_txn = PessimisticTxnConfig {
        wait_for_lock_timeout: ReadableDuration::millis(10),
        wake_up_delay_duration: ReadableDuration::millis(100),
        pipelined: true,
    };
    value.causet_context = causet_contextConfig {
        min_ts_interval: ReadableDuration::secs(4),
        old_value_cache_size: 512,
    };

    let custom = read_file_in_project_dir("integrations/config/test-custom.toml");
    let load = toml::from_str(&custom).unwrap();
    if value != load {
        diff_config(&value, &load);
    }
    let dump = toml::to_string_pretty(&load).unwrap();
    let load_from_dump = toml::from_str(&dump).unwrap();
    if load != load_from_dump {
        diff_config(&load, &load_from_dump);
    }
}

fn diff_config(lhs: &EINSTEINDBConfig, rhs: &EINSTEINDBConfig) {
    let lhs_str = format!("{:?}", lhs);
    let rhs_str = format!("{:?}", rhs);

    fn find_index(l: impl Iteron<Item = (u8, u8)>) -> usize {
        let mut it = l
            .enumerate()
            .take_while(|(_, (l, r))| l == r)
            .filter(|(_, (l, _))| *l == b' ');
        let mut last = None;
        let mut second = None;
        while let Some(a) = it.next() {
            second = last;
            last = Some(a);
        }
        second.map_or(0, |(i, _)| i)
    };
    let cpl = find_index(lhs_str.bytes().zip(rhs_str.bytes()));
    let csl = find_index(lhs_str.bytes().rev().zip(rhs_str.bytes().rev()));
    if cpl + csl > lhs_str.len() || cpl + csl > rhs_str.len() {
        assert_eq!(lhs, rhs);
    }
    let lhs_diff = String::from_utf8_lossy(&lhs_str.as_bytes()[cpl..lhs_str.len() - csl]);
    let rhs_diff = String::from_utf8_lossy(&rhs_str.as_bytes()[cpl..rhs_str.len() - csl]);
    panic!(
        "config not matched:\nlhs: ...{}...,\nrhs: ...{}...",
        lhs_diff, rhs_diff
    );
}

#[test]
fn test_serde_default_config() {
    let causet: EINSTEINDBConfig = toml::from_str("").unwrap();
    assert_eq!(causet, EINSTEINDBConfig::default());

    let content = read_file_in_project_dir("integrations/config/test-default.toml");
    let causet: EINSTEINDBConfig = toml::from_str(&content).unwrap();
    assert_eq!(causet, EINSTEINDBConfig::default());
}

#[test]
fn test_readpool_default_config() {
    let content = r#"
        [readpool.unified]
        max-thread-count = 1
    "#;
    let causet: EINSTEINDBConfig = toml::from_str(content).unwrap();
    let mut expected = EINSTEINDBConfig::default();
    expected.readpool.unified.max_thread_count = 1;
    assert_eq!(causet, expected);
}

#[test]
fn test_do_not_use_unified_readpool_with_legacy_config() {
    let content = r#"
        [readpool.causet_storage]
        normal-concurrency = 1

        [readpool.interlock]
        normal-concurrency = 1
    "#;
    let causet: EINSTEINDBConfig = toml::from_str(content).unwrap();
    assert!(!causet.readpool.is_unified_pool_enabled());
}

#[test]
fn test_block_cache_backward_compatible() {
    let content = read_file_in_project_dir("integrations/config/test-cache-compatible.toml");
    let mut causet: EINSTEINDBConfig = toml::from_str(&content).unwrap();
    assert!(causet.causet_storage.block_cache.shared);
    assert!(causet.causet_storage.block_cache.capacity.0.is_none());
    causet.compatible_adjust();
    assert!(causet.causet_storage.block_cache.capacity.0.is_some());
    assert_eq!(
        causet.causet_storage.block_cache.capacity.0.unwrap().0,
        causet.lmdb.defaultcauset.block_cache_size.0
            + causet.lmdb.writecauset.block_cache_size.0
            + causet.lmdb.lockcauset.block_cache_size.0
            + causet.violetabftdb.defaultcauset.block_cache_size.0
    );
}
