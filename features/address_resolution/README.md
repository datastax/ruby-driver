# Address resolution

Ruby driver uses plug-able address-resolution policy to map Apache Cassandra
node's ip address to another address value when necessary.

```ditaa
 Application        Ruby Driver      Address resolution policy
------+------------------+----------------------+-------------
      |                  |                      |
      |  connect         |                      |
      |----------------->|                      |
      |                  |                      |
      |             /----+-----\                |
      |             :find peers|                |
      |             \----+-----/                |
      |                  |                      |
      |    /-------------+-------------\        |
      |    :resolve each peer's address+---+    |
      |    \-------------+-------------/   |    |
      |                  |          ^      v    |
      |                  |          |   /-------+-------\
      |                  |          +---+resolve address:
      |                  |              \-------+-------/
      |                  |                      |
```

Default address resolution policy simply returns original address. This should
be enough for most cluster setups, however, can present problems in environments
like multi-region EC2, since it would force ruby-driver to use public IPs of
cassandra instances even within the same datacenter.

Therefore, Ruby driver comes with an EC2 Multi-Region address resolution policy.

## EC2 Multi-Region

This address resolution policy relies on some properties of AWS DNS to work. 

When activated, this strategy performs a forward-confirmed reverse DNS lookup
of a given peer's ip address. These addresses are public (e.g. 23.21.218.233).
This lookup will resolve to a private ip when done within datacenter and to
public ip from anywhere else. Finally, if DNS lookup fails, the policy will
return original address.

```ditaa
Ruby Driver   EC2 Multi–Region Policy                                    AWS DNS
-----+------------------+---------------------------------------------------+---
     |                  |                                                   |
     |  23.21.218.233   |                                                   |
     |----------------->|                                                   |
     |                  |                                                   |
     |                  |  233.218.21.23.in–addr.arpa PTR                   |
     |                  |-------------------------------------------------->|
     |                  |                                                   |
     |                  |        ec2–23–21–218–233.compute–1.amazonaws.com  |
     |                  |<--------------------------------------------------|
     |                  |                                                   |
     |                  |  ec2–23–21–218–233.compute–1.amazonaws.com A      |
     |                  |-------------------------------------------------->|
     |                  |                                                   |
     |                  |                                      172.31.14.4  |
     |                  |<--------------------------------------------------|
     |                  |                                                   |
     |     172.31.14.4  |                                                   |
     |<-----------------|                                                   |
     |                  |                                                   |
```

__Note__: This policy uses blocking DNS lookups internally and may hang the
reactor for the duration of the lookup. Fortunately, these address resolutions
happen only during initial connect or host additions/recoveries.

Also note that the policy will resolve to the first successfully looked up IP
address. It should present no problems on EC2, but is worth mentioning
explicitly.

To enable EC2 Multi-Region address resolution policy, use the following:

```ruby
require 'cassandra'

cluster = Cassandra.cluster(address_resolution: :ec2_multi_region)
```
