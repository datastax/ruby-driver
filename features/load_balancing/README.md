# Load Balancing

Load balancing policies are responsible for routing requests, determining which nodes the driver must connect to as well as the order in which different hosts will be tried in case of network failures. Load balancing policies therefore must also be [state listeners](http://datastax.github.io/ruby-driver/features/state_listeners/) and receive notifications about [cluster membership and availability changes](http://datastax.github.io/ruby-driver/features/state_listeners/membership_changes/).

Each cluster can be configured with a specific load balancing policy to be used.
And the same policy will be used for all requests across all sessions managed by that cluster instance.

Here is a high level diagram of how a load balancing policy is used during query execution:

```ditaa
 Application          Session    Load Balancing Policy    Execution Plan
------+------------------+-----------------+-----------------------------
      |                  |                 |
      |  execute         |                 |
      |----------------->|                 |
      |                  |  plan           |
      |                  |---------------->|
      |                  |           /-----+-----\
      |                  |           :create plan+------------->|
      |                  |           \-----+-----/              |
      |    /-------------+-------------\   |                    |
      |    :try each host until success+--------------------+   |
      |    \-------------+-------------/   |                |   |
      |                  |          ^      |                v   |
      |                  |          |      |             /------+-------\
      |                  |          +--------------------+find next host:
      |   return result  |                 |             \------+-------/
      |<-----------------|                 |                    |
      |                  |                 |
```

