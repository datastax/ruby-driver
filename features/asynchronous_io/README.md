# Asynchronous IO

[Cassandra's native binary protocol supports request pipelining](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v4.spec#L141). Essentially, this lets a single connection to be used for several simultaneous and independent request/response exchanges. Additionally, Ruby Driver doesn't use any blocking apis internally and runs all requests in the background reactor thread.

```ditaa
/------+                          /------+
|Client|                          |Server|
+---+--/                          +---+--/
    :                                 :
    |-------------------------------->|
    |        request 1                |
    |-------------------------------->|
    |        request 2                |
    |                                 |
    |                                 |
    |<--------------------------------|
    |                response 2       |
    |                                 |
    |                                 |
    |                                 |
    |-------------------------------->|
    |        request 3                |
    |                                 |
    |<--------------------------------|
    |                response 1       |
    |                                 |
    |                                 |
    |<--------------------------------|
    |                response 3       |
    |                                 |
    +                                 +
```

For consistency of API, all asynchronous methods end with `_async` (e.g. [Session#execute_async](http://datastax.github.io/ruby-driver/api/session/#execute_async-instance_method)) and return a [Future](http://datastax.github.io/ruby-driver/api/future/) object.

Methods like [Session#prepare](http://datastax.github.io/ruby-driver/api/session/#prepare-instance_method), [Session#execute](http://datastax.github.io/ruby-driver/api/session/#execute-instance_method) and [Session#close](http://datastax.github.io/ruby-driver/api/session/#close-instance_method) are thin wrappers around [Session#prepare_async](http://datastax.github.io/ruby-driver/api/session/#prepare_async-instance_method), [Session#execute_async](http://datastax.github.io/ruby-driver/api/session/#execute_async-instance_method) and [Session#close_async](http://datastax.github.io/ruby-driver/api/session/#close_async-instance_method) accordingly. These wrapper methods simply call their asynchronous counter part and block waiting for resulting future to be resolved.

A [`Cassandra::Future`](http://datastax.github.io/ruby-driver/api/future/) can be used to:

  * block application thread until execution has completed
  * register a listener to be notified when a result is available.

When describing different asynchronous method results, we will use a `Cassandra::Future<Type>` notation to signal the type of the result of the future. For example, `Cassandra::Future<Cassandra::Result>` is a future that returns an instance of [`Cassandra::Result`](http://datastax.github.io/ruby-driver/api/result/) when calling its `#get` method.

### Example: getting a result

```ruby
future = session.execute_async(statement)
result = future.get # will block and raise error or return result
```

Whenever a Future is resolved using its [`Cassandra::Future#get`](http://datastax.github.io/ruby-driver/api/future/#get-instance_method) method, it will block until it has a value. Once a value is available, it will be returned. In case of an error, an exception will be raised.

### Example: registering a listener

```ruby
future = session.execute_async(statement)

# register success listener
future.on_success do |rows|
  rows.each do |row|
    puts "#{row["artist"]}: #{row["title"]} / #{row["album"]}"
  end
end
```
