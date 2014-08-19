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
          @out.puts("Host #{host.ip} is found")
        end

        def host_lost(host)
          @out.puts("Host #{host.ip} is lost")
        end

        def host_up(host)
          @out.puts("Host #{host.ip} is up")
        end

        def host_down(host)
          @out.puts("Host #{host.ip} is down")
        end
      end
      """
    And the following example running in the background:
      """ruby
      require 'printing_listener'
      require 'cql'

      listener = PrintingListener.new($stderr)
      cluster  = Cql.connect

      at_exit { cluster.close }

      cluster.register(listener)

      sleep
      """

  Scenario: some existing hosts are terminated
    When node 3 stops
    And node 2 stops
    Then background output should contain:
      """
      Host 127.0.0.3 is down
      Host 127.0.0.2 is down
      """

  Scenario: a new host joins and then leaves the cluster
    When node 4 joins
    And node 4 leaves
    Then background output should contain:
      """
      Host 127.0.0.4 is found
      Host 127.0.0.4 is up
      Host 127.0.0.4 is down
      Host 127.0.0.4 is lost
      """
