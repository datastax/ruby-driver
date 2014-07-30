# Asynchronous IO

[Cassandra's native binary protocol supports request pipelining](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec#L117). Essentially, this lets a single connection to be used for several simultaneous and independent request/response exchanges. This makes asynchronous execution support a breeze to implement. Additionally, Ruby Driver doesn't use any blocking apis internally and runs all requests in the background reactor thread.

To make it easy to distinguish synchronous vs asynchronous methods, all asynchronous methods look like `*_async` (e.g. `execute_async`) and return a `Cql::Future` object.

`Cql::Session` methods like `prepare`, `execute` and `close` are thin wrappers around `prepare_async`, `execute_async` and `close_async` accordingly. These wrapper methods simply call their asynchronous counter part and block until resulting future is resolved.

A `Cql::Future` can be used to:

  * block application thread until execution has completed
  * register a listener to be notified when a result is available.
