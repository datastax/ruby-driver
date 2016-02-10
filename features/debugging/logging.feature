Feature: Logging

  Cluster object allows registering loggers. It then uses these to log the driver's
  actions. The library's `Cassandra::Logger` can be used to retrieve the timestamp,
  thread-id, severity, and message.

  Background:
    Given a running cassandra cluster

  Scenario: Logging is enabled using internal logger
    Given the following example:
      """ruby
      require 'cassandra'

      logger    = Cassandra::Logger.new($stderr)
      cluster   = Cassandra.cluster(logger: logger)
      session   = cluster.connect
      """
    When it is executed
    Then its output should contain:
      """
      DEBUG: Host 127.0.0.1 is found and up
      """
    And its output should contain:
      """
      INFO: Schema refreshed
      """
    And its output should contain:
      """
      INFO: Session created
      """