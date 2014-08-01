Feature: Request tracing

  Execution information can be used to access request trace if tracing was enabled.

  Background:
    Given a running cassandra cluster with a keyspace "simplex" and a table "songs"

  Scenario: tracing is disabled by default
    Given the following example:
      """ruby
      require 'cql'

      cluster   = Cql.cluster.build
      session   = cluster.connect("simplex")
      execution = session.execute("SELECT * FROM songs").execution_info

      at_exit { cluster.close }

      if execution.trace
        puts "failure"
      else
        puts "success"
      end
      """
    When it is executed
    Then its output should contain:
      """
      success
      """

  Scenario: tracing is enabled explicitly
    Given the following example:
      """ruby
      require 'cql'

      cluster   = Cql.cluster.build
      session   = cluster.connect("simplex")
      execution = session.execute("SELECT * FROM songs", :trace => true).execution_info
      trace     = execution.trace

      at_exit { cluster.close }

      puts "coordinator: #{trace.coordinator}"
      puts "started at: #{trace.started_at}"
      puts "total events: #{trace.events.size}"
      puts "parameters: #{trace.parameters.inspect}"
      puts "request: #{trace.request}"
      """
    When it is executed
    Then its output should match:
      """
      coordinator: 127\.0\.0\.(1|2|3)
      """
    And its output should match:
      """
      started at: \d{4}\-\d{2}\-\d{2} \d{2}:\d{2}:\d{2} (-|\+)\d{4}
      """
    And its output should match:
      """
      total events: \d+
      """
    And its output should contain:
      """
      parameters: {"page_size"=>"50000", "query"=>"SELECT * FROM songs"}
      request: Execute CQL3 query
      """
