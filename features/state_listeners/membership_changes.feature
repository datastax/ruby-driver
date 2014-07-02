# encoding: utf-8

Feature: membership change detection

  Cluster object allows registering state listeners. It then gurantees that
  they will be notifies on cluster membership changes.

  Background:
    Given a running cassandra cluster
    And a file named "printing_listener.rb" with:
      """ruby
      class PrintingListener
        def initialize(io)
          @out = io
        end

        def host_found(host)
          @out.puts("Host #{host.address.inspect} has been found")
        end

        def host_lost(host)
          @out.puts("Host #{host.address.inspect} has been lost")
        end

        def host_up(host)
          @out.puts("Host #{host.address.inspect} is up")
        end

        def host_down(host)
          @out.puts("Host #{host.address.inspect} is down")
        end
      end
      """
    And the following example running in the background:
      """ruby
      require 'printing_listener'
      require 'cql'

      listener = PrintingListener.new($stderr)
      cluster  = Cql.cluster             \
                  .with_contact_points(["127.0.0.1"]) \
                  .build

      at_exit { cluster.close }

      cluster.register(listener)

      sleep
      """

  Scenario: some existing hosts are terminated
    When node 3 stops
    And node 2 stops
    Then background output should contain:
      """
      Host 127.0.0.3:9042 is down
      Host 127.0.0.2:9042 is down
      """

  Scenario: a new host joins and then leaves the cluster
    When node 4 joins
    And node 4 leaves
    Then background output should contain:
      """
      Host 127.0.0.4:9042 has been found
      Host 127.0.0.4:9042 has been lost
      """
