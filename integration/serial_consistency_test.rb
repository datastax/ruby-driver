# encoding: utf-8

#--
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require File.dirname(__FILE__) + '/integration_test_case.rb'

class SerialConsistencyTest < IntegrationTestCase
  def self.before_suite
    @@ccm_cluster = CCM.setup_cluster(2, 3)
  end

  def setup_schema
    @@ccm_cluster.setup_schema(<<-CQL)
    CREATE KEYSPACE simplex WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3', 'dc2': '3'};
    USE simplex;
    CREATE TABLE users (user_id BIGINT PRIMARY KEY, first VARCHAR, last VARCHAR, age BIGINT);
    INSERT INTO users (user_id, first, last, age) VALUES (0, 'John', 'Doe', 40);
    INSERT INTO users (user_id, first, last, age) VALUES (1, 'Mary', 'Doe', 35);
    INSERT INTO users (user_id, first, last, age) VALUES (2, 'Agent', 'Smith', 32);
    INSERT INTO users (user_id, first, last, age) VALUES (3, 'Apache', 'Cassandra', 7);
    CQL
  end

  # Test for executing with serial serial_consistency
  #
  # test_can_use_serial_consistency tests that :serial serial_consistency can be used when performing a write operation
  # into a Cassandra cluster. It first executes a simple statement with :serial serial_consistency and verifies that the
  # row is updated. It then executes a prepared statement with :serial serial_consistency and verifies that the row is
  # updated.
  #
  # @since 2.1.4
  # @jira_ticket RUBY-114
  # @expected_result Each query should be executed with :serial serial_consistency
  #
  # @test_assumptions A Cassandra cluster with version 2.0.0 or higher.
  # @test_category consistency:serial
  #
  def test_can_use_serial_consistency
    skip("Serial_consistency in requests is available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    setup_schema
    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Simple statement
      result = session.execute("UPDATE users SET first = 'Joss', last = 'Fillion', age = 41 WHERE user_id = 0 IF first = 'John'",
                              serial_consistency: :serial, consistency: :all)
      assert_equal :serial, result.execution_info.options.serial_consistency

      result  = session.execute("SELECT * FROM users WHERE user_id = 0").first
      assert_equal({"user_id"=>0, "age"=>41, "first"=>"Joss", "last"=>"Fillion"}, result)

      # Prepared statement
      update = Retry.with_attempts(5) { session.prepare("UPDATE simplex.users SET first = 'John', last = 'Doe', age = 40 WHERE user_id = ? IF first = 'Joss'") }
      result = Retry.with_attempts(5) { session.execute(update, arguments: [0], serial_consistency: :serial, consistency: :all) }
      assert_equal :serial, result.execution_info.options.serial_consistency

      select = Retry.with_attempts(5) { session.prepare("SELECT * FROM simplex.users WHERE user_id = ?") }
      result = Retry.with_attempts(5) { session.execute(select, arguments: [0]).first }
      assert_equal({"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}, result)
    ensure
      cluster && cluster.close
    end
  end

  # Test for executing with local_serial serial_consistency
  #
  # test_can_use_serial_consistency tests that :local_serial serial_consistency can be used when performing a write operation
  # into a Cassandra cluster. It first executes a simple statement with :local_serial serial_consistency and verifies that the
  # row is updated. It then executes a prepared statement with :local_serial serial_consistency and verifies that the row is
  # updated.
  #
  # @since 2.1.4
  # @jira_ticket RUBY-114
  # @expected_result Each query should be executed with :local_serial serial_consistency
  #
  # @test_assumptions A Cassandra cluster with version 2.0.0 or higher.
  # @test_category consistency:serial
  #
  def test_can_use_local_serial_consistency
    skip("Serial_consistency in requests is available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    setup_schema
    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      # Simple statement
      result = session.execute("UPDATE users SET first = 'Joss', last = 'Fillion', age = 41 WHERE user_id = 0 IF first = 'John'",
                               serial_consistency: :local_serial, consistency: :all)
      assert_equal :local_serial, result.execution_info.options.serial_consistency

      result  = session.execute("SELECT * FROM users WHERE user_id = 0").first
      assert_equal({"user_id"=>0, "age"=>41, "first"=>"Joss", "last"=>"Fillion"}, result)

      # Prepared statement
      update = Retry.with_attempts(5) { session.prepare("UPDATE simplex.users SET first = 'John', last = 'Doe', age = 40 WHERE user_id = ? IF first = 'Joss'") }
      result = Retry.with_attempts(5) { session.execute(update, arguments: [0], serial_consistency: :local_serial, consistency: :all) }
      assert_equal :local_serial, result.execution_info.options.serial_consistency

      select = Retry.with_attempts(5) { session.prepare("SELECT * FROM simplex.users WHERE user_id = ?") }
      result = Retry.with_attempts(5) { session.execute(select, arguments: [0]).first }
      assert_equal({"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}, result)
    ensure
      cluster && cluster.close
    end
  end

  # Test for executing with local_serial serial_consistency on remote dc
  #
  # test_raise_error_on_local_serial_consistency_if_primary_down tests that :local_serial serial_consistency cannot be
  # used when performing a write operation into a Cassandra cluster if the local dc does not converge to QUORUM. It first
  # stops 2 out of 3 nodes in dc1. It then executes a simple statement with :local_serial serial_consistency and verifies
  # that a Cassandra::Errors::UnavailableError is raised. It then repeats this with a prepared statement.
  #
  # @expected_errors [Cassandra::Errors::UnavailableError] When writing to the local dc with < QUORUM available
  # @since 2.1.4
  # @jira_ticket RUBY-114
  # @expected_result Each query should raise Cassandra::Errors::UnavailableError with executed with :local_serial serial_consistency
  #
  # @test_assumptions A Cassandra cluster with version 2.0.0 or higher.
  # @test_assumptions A 2dc Cassandra cluster with 3 nodes each, and RF=3 for each dc
  # @test_category consistency:serial
  #
  def test_raise_error_on_local_serial_consistency_if_primary_down
    skip("Serial_consistency in requests is available in C* after 2.0") if CCM.cassandra_version < '2.0.0'

    setup_schema
    begin
      base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new('dc1')
      policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
      cluster = Cassandra.cluster(load_balancing_policy: policy)
      session = cluster.connect("simplex")

      # Bring down dc1, leave one node up for CL=1 and dc1 is used
      @@ccm_cluster.stop_node("node1")
      @@ccm_cluster.stop_node("node2")

      # Simple statement
      begin
        session.execute("UPDATE users SET first = 'Joss', last = 'Fillion', age = 41 WHERE user_id = 0 IF first = 'John'",
                        consistency: :local_one, serial_consistency: :local_serial)
      rescue Cassandra::Errors::NoHostsAvailable => e
        raise e unless e.errors.first.last.is_a?(Cassandra::Errors::UnavailableError)
      end

      # Prepared statement
      update = Retry.with_attempts(5) { session.prepare("UPDATE simplex.users SET first = 'John', last = 'Doe', age = 40 WHERE user_id = ? IF first = 'Joss'") }
      begin
        Retry.with_attempts(5, Cassandra::Errors::InvalidError) { session.execute(update, arguments: [0], consistency: :local_one, serial_consistency: :local_serial) }
      rescue Cassandra::Errors::NoHostsAvailable => e
        raise e unless e.errors.first.last.is_a?(Cassandra::Errors::UnavailableError)
      end
    ensure
      cluster && cluster.close
    end
  end

  # Test for executing with serial and local_serial serial_consistency with batches
  #
  # test_can_use_serial_consistency_in_batch_statements tests that :serial serial_consistency and :local_serial
  # serial_consistency with a batch statement can be used when performing a write operation into a Cassandra cluster.
  # It first executes a simple batch statement with :serial serial_consistency and verifies that the row is updated.
  # It then executes a prepared statement with :serial batch serial_consistency and verifies that the row is updated.
  # Finally, it repeats this entire process with :local_serial serial_consistency
  #
  # @since 2.1.4
  # @jira_ticket RUBY-114
  # @expected_result Each batch query should be executed with :serial and :local_serial serial_consistency
  #
  # @test_assumptions A Cassandra cluster with version 2.1.0 or higher.
  # @test_category consistency:serial
  #
  def test_can_use_serial_consistency_in_batch_statements
    skip("Serial_consistency in batches is available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    setup_schema
    begin
      cluster = Cassandra.cluster
      session = cluster.connect("simplex")

      ## serial serial_consistency
      # Simple statements
      simple_batch = session.batch do |b|
        b.add("UPDATE users SET first = 'Joss', last = 'Fillion', age = 41 WHERE user_id = 0 IF first = 'John'")
      end
      result = session.execute(simple_batch, serial_consistency: :serial, consistency: :all)
      assert_equal :serial, result.execution_info.options.serial_consistency

      sleep(1) # sleep is needed here for batch to propagate across the schema
      result  = session.execute("SELECT * FROM users WHERE user_id = 0").first
      assert_equal({"user_id"=>0, "age"=>41, "first"=>"Joss", "last"=>"Fillion"}, result)

      # Prepared statements
      update = Retry.with_attempts(5) { session.prepare("UPDATE simplex.users SET first = 'John', last = 'Doe', age = 40 WHERE user_id = ? IF first = 'Joss'") }
      prepared_batch = session.batch do |b|
        b.add(update, arguments: [0])
      end
      result = Retry.with_attempts(5) { session.execute(prepared_batch, serial_consistency: :serial, consistency: :all) }
      assert_equal :serial, result.execution_info.options.serial_consistency

      sleep(1)
      select = Retry.with_attempts(5) { session.prepare("SELECT * FROM simplex.users WHERE user_id = ?") }
      result = Retry.with_attempts(5) { session.execute(select, arguments: [0]).first }
      assert_equal({"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}, result)

      ## local_serial serial_consistency
      # Simple statements
      result = session.execute(simple_batch, serial_consistency: :local_serial, consistency: :all)
      assert_equal :local_serial, result.execution_info.options.serial_consistency

      sleep(1)
      result  = session.execute("SELECT * FROM users WHERE user_id = 0").first
      assert_equal({"user_id"=>0, "age"=>41, "first"=>"Joss", "last"=>"Fillion"}, result)

      # Prepared statements
      result = session.execute(prepared_batch, serial_consistency: :local_serial, consistency: :all)
      assert_equal :local_serial, result.execution_info.options.serial_consistency

      sleep(1)
      result  = session.execute(select, arguments: [0]).first
      assert_equal({"user_id"=>0, "age"=>40, "first"=>"John", "last"=>"Doe"}, result)
    ensure
      cluster && cluster.close
    end
  end

  # Test for executing with local_serial serial_consistency in a batch on remote dc
  #
  # test_raise_error_on_local_serial_consistency_in_batches_if_primary_down tests that :local_serial serial_consistency
  # with a batch statement cannot be used when performing a write operation into a Cassandra cluster if the local dc does
  # not converge to QUORUM. It first stops 2 out of 3 nodes in dc1. It then executes a simple batch statement with
  # :local_serial serial_consistency and verifies that a Cassandra::Errors::UnavailableError is raised. It then repeats
  # this with a prepared batch statement.
  #
  # @expected_errors [Cassandra::Errors::UnavailableError] When writing to the local dc with < QUORUM available
  # @since 2.1.4
  # @jira_ticket RUBY-114
  # @expected_result Each query should raise Cassandra::Errors::UnavailableError with executed with :local_serial serial_consistency
  #
  # @test_assumptions A Cassandra cluster with version 2.1.0 or higher.
  # @test_assumptions A 2dc Cassandra cluster with 3 nodes each, and RF=3 for each dc
  # @test_category consistency:serial
  #
  def test_raise_error_on_local_serial_consistency_in_batches_if_primary_down
    skip("Serial_consistency in batches is available in C* after 2.1") if CCM.cassandra_version < '2.1.0'

    setup_schema
    begin
      base_policy = Cassandra::LoadBalancing::Policies::DCAwareRoundRobin.new('dc1')
      policy = Cassandra::LoadBalancing::Policies::TokenAware.new(base_policy)
      cluster = Cassandra.cluster(load_balancing_policy: policy)
      session = cluster.connect("simplex")

      # Bring down dc1, leave one node up for CL=1 and dc1 is used
      @@ccm_cluster.stop_node("node1")
      @@ccm_cluster.stop_node("node2")

      # Simple statement
      simple_batch = session.batch do |b|
        b.add("UPDATE users SET first = 'Joss', last = 'Fillion', age = 41 WHERE user_id = 0 IF first = 'John'")
      end
      begin
        Retry.with_attempts(5, Cassandra::Errors::WriteTimeoutError) do
          session.execute(simple_batch, consistency: :local_one, serial_consistency: :local_serial)
        end
      rescue Cassandra::Errors::NoHostsAvailable => e
        raise e unless e.errors.first.last.is_a?(Cassandra::Errors::UnavailableError)
      end

      # Prepared statement
      update = Retry.with_attempts(5) { session.prepare("UPDATE simplex.users SET first = 'Joss', last = 'Fillion', age = 41 WHERE user_id = ? IF first = 'John'") }
      prepared_batch = session.batch do |b|
        b.add(update, arguments: [0])
      end
      begin
        Retry.with_attempts(5, Cassandra::Errors::InvalidError, Cassandra::Errors::WriteTimeoutError) do
          session.execute(prepared_batch, consistency: :local_one, serial_consistency: :local_serial)
        end
      rescue Cassandra::Errors::NoHostsAvailable => e
        raise e unless e.errors.first.last.is_a?(Cassandra::Errors::UnavailableError)
      end
    ensure
      cluster && cluster.close
    end
  end

end
