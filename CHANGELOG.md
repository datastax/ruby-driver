# 1.0.0.beta.2

Features:

* TokenAware load balancing policy

Bug fixes:

* [RUBY-8] correctly update host status when down/up events received immediately after each other

# 1.0.0.beta.1

Features:

* Fully asynchronous API
* Single cluster, multiple sessions
* New statements API (Simple, Prepared, Bound and Batch)
* Per-request execution information and tracing
* Base set of policies for load balancing, retry and reconnection as well as ability to write your own
* Host and Schema metadata and state listeners
