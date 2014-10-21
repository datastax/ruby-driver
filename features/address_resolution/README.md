# Address resolution

Ruby driver uses plug-able address-resolution policy to map Apache Cassandra
node's ip address to another address value when necessary.

Consider a Cassandra multi-region setup on EC2. All nodes in this setup expose
their public ip addresses in Cassandra's system tables by default. Using this
information only, all clients, regardless if they were in the same datacenter
as some of the nodes or not, would connect using public ip addresses. This
might not be desireable under certain circumstances.

Therefore, Ruby driver comes with an EC2 address resolution strategy. This
strategy relies on some properties of AWS DNS to work. When activated, this
strategy performs a reverse DNS lookup of a given ip address, remember that
these addresses are usually public (e.g. 23.21.218.233) and gets and EC2
hostname (e.g. ec2-23-21-218-233.compute-1.amazonaws.com). It then uses this
hostname to resolve an ip address. This hostname will resolve to a private ip
if looked up inside the same datacenter and to public ip otherwise.

To enable EC2 Multi Region address resolution policy, use the following:

```ruby
require 'cassandra'

cluster = Cassandra.cluster(address_resolution: :ec2_multi_region)
```
