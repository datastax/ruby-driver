# Asynchronous IO

[Cassandra's native binary protocol supports request pipelining](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec#L117). Essentially, this lets a single connection to be used for several simultaneous and independent request/response exchanges. Additionally, Ruby Driver doesn't use any blocking apis internally and runs all requests in the background reactor thread.

To make it easy to distinguish synchronous vs asynchronous methods, all asynchronous methods end with `_async` (e.g. `execute_async`) and return a `Cql::Future` object.

`Cql::Session` methods like `prepare`, `execute` and `close` are thin wrappers around `prepare_async`, `execute_async` and `close_async` accordingly. These wrapper methods simply call their asynchronous counter part and block waiting for resulting future to be resolved.

A `Cql::Future` can be used to:

  * block application thread until execution has completed
  * register a listener to be notified when a result is available.

Whenever a `Cql::Future` is resolved using its `get` method, it will block until it has a value. Once a value is available, it will be returned. In case of an error, an exception will be raised.

When describing different asynchronous method results, we will use a `Cql::Future[Type]` notation to signal the type of the result of the future. For example, `Cql::Future[Cql::Result]` is a future that returns an instance of `Cql::Result` when calling its `get` method.
