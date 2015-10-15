CREATE KEYSPACE system WITH replication = {'class': 'LocalStrategy'} AND durable_writes = true;

CREATE TABLE system."HintsColumnFamily" (
  key blob,
  column1 blob,
  value blob,
  PRIMARY KEY (key, column1)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'unused'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system."IndexInfo" (
  table_name varchar,
  index_name varchar,
  PRIMARY KEY (table_name, index_name)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'indexes that have been completed'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system."LocationInfo" (
  key blob,
  column1 blob,
  value blob,
  PRIMARY KEY (key, column1)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'unused'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system."Migrations" (
  key blob,
  column1 timeuuid,
  value blob,
  PRIMARY KEY (key, column1)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'unused'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system."NodeIdInfo" (
  key varchar,
  id timeuuid,
  PRIMARY KEY (key, id)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'counter node IDs'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system."Schema" (
  key blob,
  column1 varchar,
  value blob,
  PRIMARY KEY (key, column1)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'unused'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.batchlog (
  id uuid PRIMARY KEY,
  data blob,
  written_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'uncommited batches'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': '2'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.hints (
  target_id uuid,
  hint_id timeuuid,
  message_version int,
  mutation blob,
  PRIMARY KEY (target_id, hint_id, message_version)
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'hints awaiting delivery'
 AND compaction = {'class': 'SizeTieredCompactionStrategy', 'min_threshold': '0', 'max_threshold': '0'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.local (
  key varchar PRIMARY KEY,
  bootstrapped varchar,
  cluster_name varchar,
  cql_version varchar,
  data_center varchar,
  gossip_generation int,
  host_id uuid,
  partitioner varchar,
  rack varchar,
  release_version varchar,
  schema_version uuid,
  thrift_version varchar,
  tokens set<varchar>,
  truncated_at map<uuid, blob>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'information about the local node'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.peer_events (
  peer inet PRIMARY KEY,
  hints_dropped map<uuid, int>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'cf contains events related to peers'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.peers (
  peer inet PRIMARY KEY,
  data_center varchar,
  host_id uuid,
  rack varchar,
  release_version varchar,
  rpc_address inet,
  schema_version uuid,
  tokens set<varchar>
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'known peers in the cluster'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.range_xfers (
  token_bytes blob PRIMARY KEY,
  requested_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'ranges requested for transfer here'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.schema_columnfamilies (
  keyspace_name varchar,
  columnfamily_name varchar,
  bloom_filter_fp_chance double,
  caching varchar,
  column_aliases varchar,
  comment varchar,
  compaction_strategy_class varchar,
  compaction_strategy_options varchar,
  comparator varchar,
  compression_parameters varchar,
  default_validator varchar,
  gc_grace_seconds int,
  id int,
  key_alias varchar,
  key_aliases varchar,
  key_validator varchar,
  local_read_repair_chance double,
  max_compaction_threshold int,
  min_compaction_threshold int,
  populate_io_cache_on_flush boolean,
  read_repair_chance double,
  replicate_on_write boolean,
  subcomparator varchar,
  type varchar,
  value_alias varchar,
  PRIMARY KEY (keyspace_name, columnfamily_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'ColumnFamily definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 604800
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.schema_columns (
  keyspace_name varchar,
  columnfamily_name varchar,
  column_name varchar,
  component_index int,
  index_name varchar,
  index_options varchar,
  index_type varchar,
  validator varchar,
  PRIMARY KEY (keyspace_name, columnfamily_name, column_name)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'ColumnFamily column attributes'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 604800
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system.schema_keyspaces (
  keyspace_name varchar PRIMARY KEY,
  durable_writes boolean,
  strategy_class varchar,
  strategy_options varchar
)
WITH COMPACT STORAGE
 AND bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'keyspace definitions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 604800
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE KEYSPACE system_traces WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'} AND durable_writes = true;

CREATE TABLE system_traces.events (
  session_id uuid,
  event_id timeuuid,
  activity varchar,
  source inet,
  source_elapsed int,
  thread varchar,
  PRIMARY KEY (session_id, event_id)
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = ''
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';

CREATE TABLE system_traces.sessions (
  session_id uuid PRIMARY KEY,
  coordinator inet,
  duration int,
  parameters map<varchar, varchar>,
  request varchar,
  started_at timestamp
)
WITH bloom_filter_fp_chance = 0.01
 AND caching = 'KEYS_ONLY'
 AND comment = 'traced sessions'
 AND compaction = {'class': 'SizeTieredCompactionStrategy'}
 AND compression = {'sstable_compression': 'SnappyCompressor'}
 AND dclocal_read_repair_chance = 0.0
 AND gc_grace_seconds = 0
 AND populate_io_cache_on_flush = 'false'
 AND read_repair_chance = 0.0
 AND replicate_on_write = 'true';