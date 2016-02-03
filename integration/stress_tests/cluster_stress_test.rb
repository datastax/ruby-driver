# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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

require File.dirname(__FILE__) + '/../integration_test_case.rb'
THIS_FILE_DIR = File.dirname(__FILE__)

class ClusterStressTest < IntegrationTestCase

  # Test for cluster connection leakage
  #
  # test_clusters_should_not_leak_connections tests for connection leaks in cluster objects, and is a port of JAVA-432.
  # It creates 10 clusters and verifies that 10 control connections are created. Then it creates 1 session for each
  # cluster and verifies that each session opens another 2 connections. It then closes the sessions and checks that only
  # the control connections are open. Finally, it closes the clusters and verifies that no open connections exist.
  # This entire process is repeated 500 times.
  #
  # @param param1 [Int] Number of clusters to launch.
  #
  # @raise [RuntimeError] If a session or cluster was unable to be created or destroyed.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-60
  # @expected_result The connection counts should match the expected along the way, and there should be no open
  # connections at the end of the test.
  #
  # @test_assumptions A running Cassandra cluster with 1 node
  #
  def test_clusters_should_not_leak_connections
    # Clusters per loop (also # threads spawned)
    num_clusters = 10
    command = "ruby -rbundler/setup -r '#{THIS_FILE_DIR}/stress_helper.rb' -e 'StressHelper.new.run_cluster_helper #{num_clusters}'"

    # Loop 500 times
    (1..500).each do |num|
      begin
        pipe = IO.popen(command, 'w+')
        child_pid = pipe.gets

        # Wait for cluster creation
        until (pipe.gets).include?("READY1")
          sleep(1)
        end

        # Check 1 connection per cluster (1 control)
        check_connections(child_pid, num_clusters)
        pipe.puts("DONE1")

        # Wait for session creation
        until (pipe.gets).include?("READY2")
          sleep(1)
        end

        # Check 3 connections per cluster (1 control + 2 session)
        check_connections(child_pid, num_clusters*3)
        pipe.puts("DONE2")

        # Wait for session closings
        until (pipe.gets).include?("READY3")
          sleep(1)
        end

        # Check 1 connection per cluster (1 control)
        check_connections(child_pid, num_clusters)
        pipe.puts("DONE3")

        # Wait for cluster closings
        until (pipe.gets).include?("READY4")
          sleep(1)
        end

        # Check no open connections (all clusters and sessions closed)
        check_connections(child_pid, 0)

        pipe.close
      rescue Exception => e
        puts "Error in loop# #{num}"
        pipe.close
        raise e
      end
    end
  end

  # Test for session connection leakage
  #
  # test_sessions_should_not_leak_connections tests for connection leaks in session objects, and is a port of JAVA-432.
  # It creates a Cassandra cluster and session and verifies that the expected control and session connections are
  # created. Then it creates 500 sessions and verifies that the expected 1001 connections are open. Then it closes 250
  # sessions and verifies 501 connections are open. Then it closes 250 sessions while simultaneously opening 250
  # sessions and verfies 501 open connections. Finally, it closes the clusters and verifies that no open connections exist.
  #
  # @param param1 [Int] Number of clusters to launch.
  #
  # @raise [RuntimeError] If a session or cluster was unable to be created or destroyed.
  #
  # @since 1.0.0
  # @jira_ticket RUBY-60
  # @expected_result The connection counts should match the expected along the way, and there should be no open
  # connections at the end of the test.
  #
  # @test_assumptions A running Cassandra cluster with 1 node
  #
  def test_sessions_should_not_leak_connections
    num_clusters = 1
    command = "ruby -rbundler/setup -r '#{THIS_FILE_DIR}/stress_helper.rb' -e 'StressHelper.new.run_session_helper #{num_clusters}'"

    begin
      pipe = IO.popen(command, 'w+')
      child_pid = pipe.gets

      # Wait for cluster creation
      until (pipe.gets).include?("READY1")
        sleep(1)
      end
      # Check 1 connection (1 control)
      check_connections(child_pid, num_clusters)
      pipe.puts("DONE1")

      # Wait for session creation
      until (pipe.gets).include?("READY2")
        sleep(1)
      end
      # Check 3 connections (1 control + 2 session)
      check_connections(child_pid, num_clusters*3)
      pipe.puts("DONE2")

      # Wait for session closings
      until (pipe.gets).include?("READY3")
        sleep(1)
      end
      # Check 1 connection (1 control)
      check_connections(child_pid, num_clusters)
      pipe.puts("DONE3")

      # Wait for 500 session openings
      until (pipe.gets).include?("READY4")
        sleep(1)
      end
      # Check 1001 connections (1 control + 1000 session)
      check_connections(child_pid, 1001)
      pipe.puts("DONE4")

      # Wait for 250 session closings
      until (pipe.gets).include?("READY5")
        sleep(1)
      end
      # Check 501 connections (1 control + 500 session)
      check_connections(child_pid, 501)
      pipe.puts("DONE5")

      # Wait for 250 session closing and 250 session openings
      until (pipe.gets).include?("READY6")
        sleep(1)
      end
      # Check 501 connections (1 control + 500 session)
      check_connections(child_pid, 501)
      pipe.puts("DONE6")

      # Wait for all cluster and session closings
      until (pipe.gets).include?("READY7")
        sleep(1)
      end
      # Check no open connections (all clusters and sessions closed)
      check_connections(child_pid, 0)

      pipe.close
    rescue Exception => e
      pipe.close
      raise e
    end
  end

  def check_connections(child_pid, num_connections)
    begin
      # Verify num connections open equals num connections expected
      output = []
      IO.popen("ss -p | grep 9042 | grep ruby | grep #{child_pid}").each do |line|
        output << line.chomp
      end

      assert_equal(num_connections, output.size)
    rescue Exception => e
      raise e
    end
  end
end
