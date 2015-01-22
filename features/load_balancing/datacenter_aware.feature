Feature: Datacenter-aware Round Robin Policy

  A specialized Round Robin load balancing policy allows for querying remote
  datacenters only when all local nodes are down. This policy will round robin
  requests across hosts in the local datacenter, falling back to remote
  datacenter if necessary. The name of the local datacenter must be supplied by
  the user.

  All known remote hosts will be tried when local nodes are not available.
  However, you can configure the exact number of remote hosts that will be used
  by passing that number when constructing a policy instance.

  By default, this policy will not attempt to use remote hosts for local
  consistencies (`:local_one` or `:local_quorum`), however, it is possible to
  change that behavior via constructor.

  Background:
    Given a running cassandra cluster in 2 datacenters with 2 nodes in each
    And the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      USE simplex;
      CREATE TABLE songs (
        id uuid PRIMARY KEY,
        title text,
        album text,
        artist text,
        tags set<text>,
        data blob
      );
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         756716f7-2e54-4715-9f00-91dcbea6cf50,
         'La Petite Tonkinoise',
         'Bye Bye Blackbird',
         'Joséphine Baker',
         {'jazz', '2013'})
      ;
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         f6071e72-48ec-4fcb-bf3e-379c8a696488,
         'Die Mösch',
         'In Gold',
         'Willi Ostermann',
         {'kölsch', '1996', 'birds'}
      );
      INSERT INTO songs (id, title, album, artist, tags)
      VALUES (
         fbdf82ed-0063-4796-9c7c-a3d4f47b4b25,
         'Memo From Turner',
         'Performance',
         'Mick Jager',
         {'soundtrack', '1991'}
      );
      """

  Scenario: First seen datacenter is considered local when not explicitly given
    Given the following example:
      """ruby
      require 'cassandra'

      policy     = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new
      hosts      = ['127.0.0.3', '127.0.0.4']
      cluster    = Cassandra.cluster(hosts: hosts, load_balancing_policy: policy)
      session    = cluster.connect('simplex')

      hosts_used = 4.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end.sort.uniq

      puts hosts_used
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.3
      127.0.0.4
      """

  Scenario: Requests are automatically routed to local datacenter
    Given the following example:
      """ruby
      require 'cassandra'

      datacenter = "dc2"
      policy     = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
      cluster    = Cassandra.cluster(load_balancing_policy: policy)
      session    = cluster.connect('simplex')

      hosts_used = 4.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end.sort.uniq

      puts hosts_used
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.3
      127.0.0.4
      """

  Scenario: Requests are routed to remote datacenters if local datacenter is down
    Given the following example:
      """ruby
      require 'cassandra'

      datacenter = "dc2"
      policy     = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
      cluster    = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
      session    = cluster.connect('simplex')

      hosts_used = 4.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end.sort.uniq

      puts hosts_used
      """
    And node 3 is stopped
    And node 4 is stopped
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.2
      """

  Scenario: Requests are routed up to a maximum number of hosts in remote datacenters
    Given the following example:
      """ruby
      require 'cassandra'

      datacenter     = "dc2"
      remotes_to_try = 1
      policy         = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, remotes_to_try)
      cluster        = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
      session        = cluster.connect('simplex')

      hosts_used = 4.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end.sort.uniq

      puts "Used #{hosts_used.size} host, with ip #{hosts_used.first}"
      """
    And node 3 is stopped
    And node 4 is stopped
    When it is executed
    Then its output should match:
      """
      Used 1 host, with ip 127\.0\.0\.(1|2)
      """

  Scenario: Requests with local consistencies are not routed to remote datacenters
    Given the following example:
      """ruby
      require 'cassandra'

      datacenter = "dc2"
      policy     = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter)
      cluster    = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
      session    = cluster.connect('simplex')

      begin
        session.execute("SELECT * FROM songs", consistency: :local_one)
        puts "failure"
      rescue Cassandra::Errors::NoHostsAvailable
        puts "success"
      end
      """
    And node 3 is stopped
    And node 4 is stopped
    When it is executed
    Then its output should contain:
      """
      success
      """

  Scenario: Routing requests with local consistencies to remote datacenters
    Given the following example:
      """ruby
      require 'cassandra'

      datacenter = "dc2"
      use_remote = true
      policy     = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new(datacenter, nil, use_remote)
      cluster    = Cassandra.cluster(consistency: :one, load_balancing_policy: policy)
      session    = cluster.connect('simplex')

      hosts_used = 4.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end.sort.uniq

      puts hosts_used
      """
    And node 3 is stopped
    And node 4 is stopped
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.2
      """
