# Error Handling

Handling errors in a distributed system is a complex and complicated topic.
Ideally, we must understand all of the possible sources of failure and determine
appropriate actions to take. This section is intended to explain various known
failure modes.

## Request Execution Errors

Below is a diagram that explains request execution at a high level:

```ditaa
                             |
                     execute |
                             |
                 /-----------+-----------\
                 |get load balancing plan|
                 \-----------+-----------/
                             |
                             |
                        +----+----+
                     no |{io}     |
      +---------------=-+has next?|
      |                 |         |
      |                 +----+----+
      |                   ^  | yes
      |                   |  :
      |          +--------+  |
      |          |           v
      |          |     /------------\
      |          |     :send request|<-------+
      |          |     \-----+------/        |
      |          |           |               |
      |          |           |               |
      |          |     +-----+-----+         |
      |          | yes |{io}       |         |
      |          +---=-+host error?|         |
      |                |           |         |
      |                +-----+-----+         |
      |                      : no            |
      |                      |               |
      |                      v               |
      v              +--------------+        |
/-----------\    yes |{io}          |        |
|raise error|<-----=-+request error?|        |
\-----------/        |              |        |
      ^              +-------+------+        |
      |                      : no            |
      |                      |               |
      |                      v               |
      |              +--------------+        |
      |           no |{io}          |        |
      |         +--=-+cluster error?+        |
      |         |    |              |        |
      |         |    +-------+------+        |
      |         |            : yes           |
      |         |            |               :
      |         |            v               | yes
      |         |        +-------+        +--+---+
      |         |        |{io}   | no     |{io}  |
      |         |        |ignore?+-=----->|retry?|
      |         |        |       |        |      |
      |         |        +---+---+        +--+---+
      |         |            : yes           | no
      |         |            |               :
      |         |            v               |
      |         |     /-------------\        |
      |         +---->|return result|        |
      |               \-------------/        |
      |                                      |
      +--------------------------------------+
```

Requests resulting in host errors are automatically retried on other hosts. If
no other hosts are present in the load balancing plan, a [`Cassandra::Errors::NoHostsAvailable`](http://datastax.github.io/ruby-driver/api/errors/no_hosts_available/)
is raised that contains a map of host to host error that were seen during
request.

Additionally, if an empty load balancing plan is returned by the load balancing
policy, the request will not be attempted on any hosts.

Whenever a cluster error occurs, the [retry policy is used to decide whether to
re-raise the error, retry the request at a different consistency or ignore the
error and return empty result](http://datastax.github.io/ruby-driver/features/retry_policies/).

Finally, all other request errors, such as validation errors, are returned to
the application without retries.

Below are top-level error classes defined in the Ruby Driver classified by host,
cluster and request types:

<table class="table table-striped table-hover table-condensed">
  <thead>
    <tr>
      <th>Type</th>
      <th>Class</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="4">Host Errors</td>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/server_error/"><code>Cassandra::Errors::ServerError</code></a>
      </td>
    </tr>
    <tr>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/overloaded_error/"><code>Cassandra::Errors::OverloadedError</code></a>
      </td>
    </tr>
    <tr>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/internal_error/"><code>Cassandra::Errors::InternalError</code></a>
      </td>
    </tr>
    <tr>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/is_bootstrapping_error/"><code>Cassandra::Errors::IsBootstrappingError</code></a>
      </td>
    </tr>
    <tr>
      <td rowspan="3">Cluster Errors</td>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/write_timeout_error/"><code>Cassandra::Errors::WriteTimeoutError</code></a>
      </td>
    </tr>
    <tr>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/read_timeout_error/"><code>Cassandra::Errors::ReadTimeoutError</code></a>
      </td>
    </tr>
    <tr>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/unavailable_error/"><code>Cassandra::Errors::UnavailableError</code></a>
      </td>
    </tr>
    <tr>
      <td rowspan="3">Request Errors</td>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/validation_error/"><code>Cassandra::Errors::ValidationError</code></a>
      </td>
    </tr>
    <tr>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/client_error/"><code>Cassandra::Errors::ClientError</code></a>
      </td>
    </tr>
    <tr>
      <td>
        <a href="http://datastax.github.io/ruby-driver/api/errors/truncate_error/"><code>Cassandra::Errors::TruncateError</code></a>
      </td>
    </tr>
  </tbody>
</table>

## Connection Heartbeat

In addition to the request execution errors and timeouts, Ruby Driver performs
periodic heart beating of each open connection to detect network outages and
prevent stale connections from gathering.

Upon detecting a stale connection, Ruby Driver will automatically close it and
fail all outstanding requests with a host level error, which will force them to
be retried on other hosts as part of a normal request execution.
