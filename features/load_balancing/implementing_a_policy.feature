Feature: Implementing custom load balancing policies

  To implement a load balancing policy, mix in `Cql::LoadBalancing::Policy`
  module and provide implementations for its required methods. Currently, load
  balancing policies are required to be thread-safe.

  The object returned from the `plan` method must implement method `next`
  that returns a `Cql::Host` instance or raises `StopIteration`.

  This method will be called from multiple threads, but never in parallel and
  `Plan` doesn't have to be thread-safe. However, because it is called across
  threads, an `Enumerator` **cannot** be used as a Plan.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"

  Scenario: A policy to ignore a certain keyspace
    Given a file named "ignoring_keyspace_policy.rb" with:
      """ruby
      class IgnoringKeyspacePolicy
        include Cql::LoadBalancing::Policy

        class Plan
          def next
            raise ::StopIteration
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
      require 'cql'
      require 'ignoring_keyspace_policy'
      
      policy  = Cql::LoadBalancing::Policies::RoundRobin.new
      cluster = Cql.cluster
                  .with_load_balancing_policy(IgnoringKeyspacePolicy.new('simplex', policy))
                  .build
      session = cluster.connect('simplex')
      
      begin
        session.execute("SELECT * FROM songs")
        puts "failure"
      rescue Cql::NoHostsAvailable
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
        include Cql::LoadBalancing::Policy

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
      require 'cql'
      require 'blacklist_policy'
      
      policy  = Cql::LoadBalancing::Policies::RoundRobin.new
      cluster = Cql.cluster
                  .with_load_balancing_policy(BlackListPolicy.new(['127.0.0.2', '127.0.0.3'], policy))
                  .build
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
