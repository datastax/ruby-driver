# Retry Policies

Retry policies allow Ruby Driver to retry a request upon encountering specific types of
server errors, namely,
[write timeout](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1041-L1066),
[read timeout](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1067-L1083)
or [unavailable](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L1025-L1035).

```ditaa
      /-----------+                        /-----------+
      |application|                        |application|
      +-----+-----/                        +-----+-----/
            |                                    |
      /-----+-----+                        /-----+-----+
    +-+coordinator+-+                    +-+coordinator|
    | +-----------/ : timeout            | +-----------/
    |               |                    |
/---+---+       /---+---+            /---+---+       /-------+
|replica|       |replica|            |replica|       |replica|
+-------/       +-------/            +-------/       +-------/

  read or write timeout                     unavailable
```

## Read Timeout

When a coordinator received the request and sent the read to replica(s) but the replica(s) did not respond in time.

```ditaa
Application        Coordinator         Replica         Replica
-----+------------------+-----------------+---------------+-------------
     |                  |                 |               |
     |  select          |                 |               |
     |----------------->|                 |               |
     |             /----+-----\  read     |               |
     |             : locate   +---------->|  read         |
     |             : replicas +-----------|-------------->|
     |             \----+-----/           |               +--+
     |                  |                 |               |  |  checksum
     |                  |  send checksum  +--+            |<-+  data
     |                  |<----------------|--|------------|
     |                  |                 |  |            |
     |                  +--+              |  |            |
     |                  |  |              |  |  retrieve  |
     |                  |  |  timeout     |  |  data      |
     |                  |  |              |  |            |
     |  read timeout    |<-+              |  |            |
     |<-----------------|                 |  |            |
     |                  |                 |<-+            |
```

In this scenario, [`Cassandra::Retry::Policy#read_timeout`](http://datastax.github.io/ruby-driver/api/retry/policy/#read_timeout-instance_method) will be used to determine the desired course of action.

## Write Timeout

When a coordinator received the request and sent the write to replica(s) but the replica(s) did not respond in time.

```ditaa
Application        Coordinator         Replica         Replica
-----+------------------+-----------------+---------------+----------
     |                  |                 |               |
     |  insert          |                 |               |
     |----------------->|                 |               |
     |             /----+-----\  write    |               |
     |             : locate   +---------->|  write        |
     |             : replicas +-----------|-------------->|
     |             \----+-----/           |               +--+
     |                  |                 |               |  |  write
     |                  |  ack write      +--+            |<-+  data
     |                  |<----------------|--|------------|
     |                  |                 |  |            |
     |                  +--+              |  |            |
     |                  |  |              |  |  write     |
     |                  |  |  timeout     |  |  data      |
     |                  |  |              |  |            |
     |  write timeout   |<-+              |  |            |
     |<-----------------|                 |  |            |
     |                  |                 |<-+            |
```

In this scenario, [`Cassandra::Retry::Policy#write_timeout`](http://datastax.github.io/ruby-driver/api/retry/policy/#write_timeout-instance_method) will be used to determine the desired course of action.

## Unavailable

When the coordinator is aware there aren't enough replica online. No requests are sent to replica nodes in this scenario, because coordinator knows that the requested consistency level cannot be possibly satisfied.

```ditaa
Application        Coordinator 
-----+------------------+------
     |                  |
     |  insert          |
     |----------------->|
     |             /----+-----\
     |             : no       +
     |             : replicas +
     |             \----+-----/
     |                  |
     |    unavailable   |
     |<-----------------|
     |                  |
```

In this scenario, [`Cassandra::Retry::Policy#unavailable`](http://datastax.github.io/ruby-driver/api/retry/policy/#unavailable-instance_method) will be used to determine the desired course of action.
