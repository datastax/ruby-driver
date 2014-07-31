# Authentication

Out of the box, Ruby driver supports [Cassandra's internal authentication mechanism](http://www.datastax.com/documentation/cassandra/2.0/cassandra/security/security_config_native_authenticate_t.html). It is also possible to provide a custom authenticator implementation, refer to `Cql::Auth` module for more information.
