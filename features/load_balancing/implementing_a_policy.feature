Feature: Implementing custom load balancing policies

  To implement a load balancing policy, you must implement all of the methods
  specified in [`Cassandra::LoadBalancing::Policy`](/api/load_balancing/policy).
  Currently, load balancing policies are required to be thread-safe.

  The object returned from the `plan` method must implement all methods of
  [`Cassandra::LoadBalancing::Plan`](/api/load_balancing/plan)

  Plan will be accessed from multiple threads, but never in parallel and it
  doesn't have to be thread-safe.

  Background:
    Given a running cassandra cluster with schema:
      """sql
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

  Scenario: A policy to ignore a certain keyspace
    Given a file named "ignoring_keyspace_policy.rb" with:
      """ruby
      class IgnoringKeyspacePolicy
        class Plan
          def has_next?
            false
          end

          def next
            nil
          end
        end

        def initialize(keyspace_to_ignore, original_policy)
          @keyspace = keyspace_to_ignore
          @policy   = original_policy
        end

        def plan(keyspace, statement, options)
          if @keyspace == keyspace
            Plan.new
          else
            @policy.plan(keyspace, statement, options)
          end
        end

        def distance(host)
          @policy.distance(host)
        end

        def host_found(host)
          @policy.host_found(host)
        end

        def host_lost(host)
          @policy.host_lost(host)
        end

        def host_up(host)
          @policy.host_up(host)
        end

        def host_down(host)
          @policy.host_down(host)
        end
      end
      """
    And the following example:
      """ruby
      require 'cassandra'
      require 'ignoring_keyspace_policy'
      
      policy  = IgnoringKeyspacePolicy.new('simplex', Cassandra::LoadBalancing::Policies::RoundRobin.new)
      cluster = Cassandra.connect(load_balancing_policy: policy)
      session = cluster.connect('simplex')
      
      begin
        session.execute("SELECT * FROM songs")
        puts "failure"
      rescue Cassandra::Errors::NoHostsAvailable
        puts "success"
      end
      """
    When it is executed
    Then its output should contain:
      """
      success
      """

  Scenario: A policy to ignore certain hosts
    Given a file named "blacklist_policy.rb" with:
      """ruby
      class BlackListPolicy
        def initialize(ips_to_ignore, original_policy)
          @ips    = ::Set.new
          @policy = original_policy

          ips_to_ignore.each do |ip|
            case ip
            when ::IPAddr
              @ips << ip
            when ::String
              @ips << ::IPAddr.new(ip)
            end
          end
        end

        def plan(keyspace, statement, options)
          @policy.plan(keyspace, statement, options)
        end

        def distance(host)
          @policy.distance(host)
        end

        def host_found(host)
          @policy.host_found(host) unless @ips.include?(host.ip)
        end

        def host_lost(host)
          @policy.host_lost(host) unless @ips.include?(host.ip)
        end

        def host_up(host)
          @policy.host_up(host) unless @ips.include?(host.ip)
        end

        def host_down(host)
          @policy.host_down(host) unless @ips.include?(host.ip)
        end
      end
      """
    And the following example:
      """ruby
      require 'cassandra'
      require 'blacklist_policy'
      
      policy  = BlackListPolicy.new(['127.0.0.2', '127.0.0.3'], Cassandra::LoadBalancing::Policies::RoundRobin.new)
      cluster = Cassandra.connect(load_balancing_policy: policy)
      session = cluster.connect('simplex')
      
      host_ips = cluster.hosts.map(&:ip).sort
      
      coordinator_ips = 3.times.map do
        info = session.execute("SELECT * FROM songs").execution_info
        info.hosts.last.ip
      end

      puts "Cluster hosts:"
      puts host_ips
      puts ""
      puts "Hosts used:"
      puts coordinator_ips.sort.uniq
      """
    When it is executed
    Then its output should contain:
      """
      Cluster hosts:
      127.0.0.1
      127.0.0.2
      127.0.0.3
      
      Hosts used:
      127.0.0.1
      """
