CREATE KEYSPACE system WITH replication = {'class': 'LocalStrategy'} AND durable_writes = true;

CREATE TABLE system."IndexInfo" (
  table_name text,
  index_name text,
  PRIMARY KEY (table_name, index_name)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'indexes that have been completed'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.batchlog (
  id uuid PRIMARY KEY,
  data blob,
  version int,
  written_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'uncommited batches'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': '2'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.compaction_history (
  id uuid PRIMARY KEY,
  bytes_in bigint,
  bytes_out bigint,
  columnfamily_name text,
  compacted_at timestamp,
  keyspace_name text,
  rows_merged map<int, bigint>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'show all compaction history'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 604800
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.compactions_in_progress (
  id uuid PRIMARY KEY,
  columnfamily_name text,
  inputs set<int>,
  keyspace_name text
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'unfinished compactions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.hints (
  target_id uuid,
  hint_id timeuuid,
  message_version int,
  mutation blob,
  PRIMARY KEY (target_id, hint_id, message_version)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'hints awaiting delivery'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.local (
  key text PRIMARY KEY,
  bootstrapped text,
  broadcast_address inet,
  cluster_name text,
  cql_version text,
  data_center text,
  gossip_generation int,
  host_id uuid,
  listen_address inet,
  native_protocol_version text,
  partitioner text,
  rack text,
  release_version text,
  rpc_address inet,
  schema_version uuid,
  thrift_version text,
  tokens set<text>,
  truncated_at map<uuid, blob>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'information about the local node'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.paxos (
  row_key blob,
  cf_id uuid,
  in_progress_ballot timeuuid,
  most_recent_commit blob,
  most_recent_commit_at timeuuid,
  proposal blob,
  proposal_ballot timeuuid,
  PRIMARY KEY (row_key, cf_id)
)
WITH bloom_filter_fp_chance = 0.1
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'in-progress paxos proposals'
 AND compaction = {'class': 'LeveledCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.peer_events (
  peer inet PRIMARY KEY,
  hints_dropped map<uuid, int>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'cf contains events related to peers'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.peers (
  peer inet PRIMARY KEY,
  data_center text,
  host_id uuid,
  preferred_ip inet,
  rack text,
  release_version text,
  rpc_address inet,
  schema_version uuid,
  tokens set<text>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'known peers in the cluster'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.range_xfers (
  token_bytes blob PRIMARY KEY,
  requested_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'ranges requested for transfer here'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.schema_columnfamilies (
  keyspace_name text,
  columnfamily_name text,
  bloom_filter_fp_chance double,
  caching text,
  cf_id uuid,
  column_aliases text,
  comment text,
  compaction_strategy_class text,
  compaction_strategy_options text,
  comparator text,
  compression_parameters text,
  default_time_to_live int,
  default_validator text,
  dropped_columns map<text, bigint>,
  gc_grace_seconds int,
  index_interval int,
  is_dense boolean,
  key_aliases text,
  key_validator text,
  local_read_repair_chance double,
  max_compaction_threshold int,
  max_index_interval int,
  memtable_flush_period_in_ms int,
  min_compaction_threshold int,
  min_index_interval int,
  read_repair_chance double,
  speculative_retry text,
  subcomparator text,
  type text,
  value_alias text,
  PRIMARY KEY (keyspace_name, columnfamily_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'ColumnFamily definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.schema_columns (
  keyspace_name text,
  columnfamily_name text,
  column_name text,
  component_index int,
  index_name text,
  index_options text,
  index_type text,
  type text,
  validator text,
  PRIMARY KEY (keyspace_name, columnfamily_name, column_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'ColumnFamily column attributes'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.schema_keyspaces (
  keyspace_name text PRIMARY KEY,
  durable_writes boolean,
  strategy_class text,
  strategy_options text
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'keyspace definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.schema_triggers (
  keyspace_name text,
  columnfamily_name text,
  trigger_name text,
  trigger_options map<text, text>,
  PRIMARY KEY (keyspace_name, columnfamily_name, trigger_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'triggers metadata table'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.schema_usertypes (
  keyspace_name text,
  type_name text,
  field_names list<text>,
  field_types list<text>,
  PRIMARY KEY (keyspace_name, type_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'Defined user types'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 604800
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.size_estimates (
  keyspace_name text,
  table_name text,
  range_start text,
  range_end text,
  mean_partition_size bigint,
  partitions_count bigint,
  PRIMARY KEY (keyspace_name, table_name, range_start, range_end)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'per-table primary range size estimates'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system.sstable_activity (
  keyspace_name text,
  columnfamily_name text,
  generation int,
  rate_120m double,
  rate_15m double,
  PRIMARY KEY ((keyspace_name, columnfamily_name, generation))
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'historic sstable read rates'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 0
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE KEYSPACE system_traces WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'} AND durable_writes = true;

CREATE TABLE system_traces.events (
  session_id uuid,
  event_id timeuuid,
  activity text,
  source inet,
  source_elapsed int,
  thread text,
  PRIMARY KEY (session_id, event_id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = ''
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 86400
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';

CREATE TABLE system_traces.sessions (
  session_id uuid PRIMARY KEY,
  coordinator inet,
  duration int,
  parameters map<text, text>,
  request text,
  started_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
 AND comment = 'traced sessions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'LZ4Compressor'}
 AND dclocal_read_repair_chance = 0.0
 AND default_time_to_live = 86400
 AND gc_grace_seconds = 0
 AND max_index_interval = 2048
 AND memtable_flush_period_in_ms = 3600000
 AND min_index_interval = 128
 AND read_repair_chance = 0.0
 AND speculative_retry = '99.0PERCENTILE';