Feature: Request Execution Errors

  The Ruby driver will error out and provide error messages for various errors.
  [Check out the request execution diagram in Error Handling guide](/features/error_handling/)
  and come back here for real-world examples of error handling.

  Background:
    Given a running cassandra cluster

  Scenario: Executing a statement with invalid syntax
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      USE simplex;
      CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      begin
        session.execute("INSERT INTO users (user_id, first, last, age)")
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Cassandra::Errors::SyntaxError
      """

  Scenario: Connecting to non-existent keyspace
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster

      begin
        session = cluster.connect("badkeyspace")
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Cassandra::Errors::InvalidError
      """

  Scenario: Modifying system keyspace
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("system")

      begin
        session.execute("CREATE TABLE users (user_id INT PRIMARY KEY)")
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Cassandra::Errors::UnauthorizedError
      """

  Scenario: Dropping non-existent keyspace
    Given the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect

      begin
        session.execute("DROP KEYSPACE badkeyspace")
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Cassandra::Errors::ConfigurationError
      """

  Scenario: Creating keyspace that already exists
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect

      begin
        session.execute("CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}")
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Cassandra::Errors::AlreadyExistsError
      """

  @auth
  Scenario: Connecting with invalid authentication credentials
    Given a running cassandra cluster with authentication enabled
    And the following example:
      """ruby
      require 'cassandra'

      begin
        Cassandra.cluster(
          username: 'invalidname',
          password: 'badpassword'
        )
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Cassandra::Errors::AuthenticationError
      """

  @netblock
  Scenario: Connecting to unreachable cluster
    Given node 1 is unreachable
    And the following example:
      """ruby
      require 'cassandra'

      begin
        Cassandra.cluster(connect_timeout: 1)
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When it is executed
    Then its output should contain:
      """
      Cassandra::Errors::IOError: Could not connect to 127.0.0.1:9042 within 1s
      """

  Scenario: Executing a statement when all hosts are down
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
      USE simplex;
      CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
      INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40);
      INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35);
      INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster(retry_policy: Cassandra::Retry::Policies::Fallthrough.new)
      session = cluster.connect("simplex")

      $stdout.puts("=== START ===")
      $stdout.flush
      until (input = $stdin.gets).nil? # block until closed
        query = input.chomp
        begin
          results = session.execute(query, consistency: :all, timeout: 2)
          puts results.inspect
          execution_info = results.execution_info
          $stdout.puts("Query #{query.inspect} fulfilled by #{execution_info.hosts}")
        rescue => e
          $stdout.puts("#{e.class.name}: #{e.message}")
        end
        $stdout.flush
      end
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    When it is running interactively
    And I wait for its output to contain "START"
    And all nodes are down
    And I type "SELECT * FROM simplex.users"
    And I close the stdin stream
    Then its output should contain:
      """
      Cassandra::Errors::NoHostsAvailable: All hosts down
      """

  Scenario: Executing a statement when can't achieve desired consistency
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};
      USE simplex;
      CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      query = "INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40)"
      begin
        session.execute(query, consistency: :all)
      rescue => e
        puts "#{e.class.name}: #{e.message}"
      end
      """
    When node 3 stops
    And it is executed
    Then its output should contain:
      """
      Cassandra::Errors::UnavailableError
      """

  @netblock
  Scenario: Executing a SELECT statement when replica times out
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
      USE simplex;
      CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
      INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40);
      INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35);
      INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster(retry_policy: Cassandra::Retry::Policies::Fallthrough.new)
      session = cluster.connect("simplex")

      $stdout.puts("=== START ===")
      $stdout.flush
      until (input = $stdin.gets).nil? # block until closed
        query = input.chomp
        begin
          results = session.execute(query, consistency: :all, timeout: 2)
          puts results.inspect
          execution_info = results.execution_info
          $stdout.puts("Query #{query.inspect} fulfilled by #{execution_info.hosts}")
        rescue => e
          $stdout.puts("#{e.class.name}: #{e.message}")
        end
        $stdout.flush
      end
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    When it is running interactively
    And I wait for its output to contain "START"
    And node 3 is unreachable
    And I type "SELECT * FROM simplex.users"
    And I close the stdin stream
    Then its output should contain:
      """
      Cassandra::Errors::ReadTimeoutError
      """

  @netblock
  Scenario: Executing an UPDATE statement when replica times out
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
      USE simplex;
      CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
      INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40);
      INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35);
      INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster(retry_policy: Cassandra::Retry::Policies::Fallthrough.new)
      session = cluster.connect("simplex")

      $stdout.puts("=== START ===")
      $stdout.flush
      until (input = $stdin.gets).nil? # block until closed
        query = input.chomp
        begin
          results = session.execute(query, consistency: :all, timeout: 2)
          puts results.inspect
          execution_info = results.execution_info
          $stdout.puts("Query #{query.inspect} fulfilled by #{execution_info.hosts}")
        rescue => e
          $stdout.puts("#{e.class.name}: #{e.message}")
        end
        $stdout.flush
      end
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    When it is running interactively
    And I wait for its output to contain "START"
    And node 3 is unreachable
    And I type "UPDATE simplex.users SET age=41 WHERE user_id=0"
    And I close the stdin stream
    Then its output should contain:
      """
      Cassandra::Errors::WriteTimeoutError
      """

  @netblock
  Scenario: Truncating a table when replica times out
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
      USE simplex;
      CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
      INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40);
      INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35);
      INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster(retry_policy: Cassandra::Retry::Policies::Fallthrough.new)
      session = cluster.connect("simplex")

      $stdout.puts("=== START ===")
      $stdout.flush
      until (input = $stdin.gets).nil? # block until closed
        query = input.chomp
        begin
          results = session.execute(query, consistency: :all, timeout: 2)
          puts results.inspect
          execution_info = results.execution_info
          $stdout.puts("Query #{query.inspect} fulfilled by #{execution_info.hosts}")
        rescue => e
          $stdout.puts("#{e.class.name}: #{e.message}")
        end
        $stdout.flush
      end
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    When it is running interactively
    And I wait for its output to contain "START"
    And node 3 is unreachable
    And I type "TRUNCATE simplex.users"
    And I close the stdin stream
    Then its output should contain:
      """
      Cassandra::Errors::TruncateError
      """

  @netblock
  Scenario: Executing a statement when all nodes time out
    Given the following schema:
      """cql
      CREATE KEYSPACE simplex WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
      USE simplex;
      CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
      INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40);
      INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35);
      INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32);
      """
    And the following example:
      """ruby
      require 'cassandra'

      cluster = Cassandra.cluster(retry_policy: Cassandra::Retry::Policies::Fallthrough.new)
      session = cluster.connect("simplex")

      $stdout.puts("=== START ===")
      $stdout.flush
      until (input = $stdin.gets).nil? # block until closed
        query = input.chomp
        begin
          results = session.execute(query, consistency: :all, timeout: 2)
          puts results.inspect
          execution_info = results.execution_info
          $stdout.puts("Query #{query.inspect} fulfilled by #{execution_info.hosts}")
        rescue => e
          $stdout.puts("#{e.class.name}: #{e.message}")
        end
        $stdout.flush
      end
      $stdout.puts("=== STOP ===")
      $stdout.flush
      """
    When it is running interactively
    And I wait for its output to contain "START"
    And all nodes are unreachable
    And I type "SELECT * FROM simplex.users WHERE user_id=0"
    And I close the stdin stream
    Then its output should contain:
      """
      Cassandra::Errors::NoHostsAvailable: All attempted hosts failed
      """
    And its output should contain:
      """
      127.0.0.1 (Cassandra::Errors::TimeoutError: Timed out)
      """
    And its output should contain:
      """
      127.0.0.2 (Cassandra::Errors::TimeoutError: Timed out)
      """
    And its output should contain:
      """
      127.0.0.3 (Cassandra::Errors::TimeoutError: Timed out)
      """
