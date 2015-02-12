# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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

require 'cassandra'

class StressHelper
  def run_cluster_helper(num_clusters)
    $stdout.sync = true
    $stdout.puts(Process.pid)

    # Create num_clusters clusters
    cluster_list = create_clusters_concurrently(num_clusters)
    $stdout.puts("READY1")

    until($stdin.gets).include?("DONE1")
      sleep(1)
    end

    # Create 1 session per cluster
    session_list = create_sessions_concurrently(cluster_list)
    $stdout.puts("READY2")

    until($stdin.gets).include?("DONE2")
      sleep(1)
    end

    # Close all sessions
    close_sessions_concurrently(session_list)
    $stdout.puts("READY3")

    until($stdin.gets).include?("DONE3")
      sleep(1)
    end

    # Close all clusters
    close_clusters_concurrently(cluster_list)
    $stdout.puts("READY4")
  end

  def run_session_helper(num_clusters)
    $stdout.sync = true
    $stdout.puts(Process.pid)

    # Create num_clusters clusters
    cluster_list = create_clusters_concurrently(num_clusters)
    $stdout.puts("READY1")

    until($stdin.gets).include?("DONE1")
      sleep(1)
    end

    # Create 1 session per cluster
    session_list = create_sessions_concurrently(cluster_list)
    $stdout.puts("READY2")

    until($stdin.gets).include?("DONE2")
      sleep(1)
    end

    # Close all sessions
    close_sessions_concurrently(session_list)
    $stdout.puts("READY3")

    until($stdin.gets).include?("DONE3")
      sleep(1)
    end

    # Create 500 sessions
    session_list = create_sessions_concurrently2(cluster_list[0], 500)
    $stdout.puts("READY4")

    until($stdin.gets).include?("DONE4")
      sleep(1)
    end

    # Close 250 sessions
    session_list = close_sessions_concurrently2(session_list, 250)
    $stdout.puts("READY5")

    until($stdin.gets).include?("DONE5")
      sleep(1)
    end

    # Close 250 sessions and open 250 new sessions
    session_list2 = create_sessions_concurrently2(cluster_list[0], 250)
    empty_session_list = close_sessions_concurrently2(session_list, 250)
    $stdout.puts("READY6")

    until($stdin.gets).include?("DONE6")
      sleep(1)
    end

    # Close all clusters and sessions
    session_list = close_sessions_concurrently2(session_list2, 250)
    close_clusters_concurrently(cluster_list)
    $stdout.puts("READY7")
  end

  def create_clusters_concurrently(num_clusters)
    cluster_list = []
    threads = (1..num_clusters).map do
      Thread.new do
        begin
          cluster = Cassandra.cluster
          cluster_list << cluster
        rescue Exception => e
          cluster.close
          raise RuntimeError.new("Error while creating a cluster. #{e.class.name}: #{e.message}
                                          Backtrace: #{e.backtrace.inspect}")
        end
      end
    end

    threads.each {|th| th.join}
    cluster_list
  end

  def create_sessions_concurrently(cluster_list)
    sessions = []
    threads = cluster_list.map do |cluster|
      Thread.new do
        begin
          session = cluster.connect
          sessions << session
        rescue Exception => e
          session.close
          raise RuntimeError.new("Error while creating a session. #{e.class.name}: #{e.message}
                                          Backtrace: #{e.backtrace.inspect}")
        end
      end
    end

    threads.each {|th| th.join}
    sessions
  end

  # This will launch up to 500 threads
  def create_sessions_concurrently2(cluster, num_sessions)
    sessions = []
    threads = (1..num_sessions).map do
      Thread.new do
        begin
          session = cluster.connect
          sessions << session
        rescue Exception => e
          session.close
          raise RuntimeError.new("Error while creating a session. #{e.class.name}: #{e.message}
                                          Backtrace: #{e.backtrace.inspect}")
        end
      end
    end

    threads.each {|th| th.join}
    sessions
  end

  def close_sessions_concurrently(session_list)
    threads = session_list.map do |session|
      Thread.new do
        begin
          session.close
        rescue Exception => e
          raise RuntimeError.new("Error while closing a session. #{e.class.name}: #{e.message}
                                  Backtrace: #{e.backtrace.inspect}")
        end
      end
    end

    threads.each {|th| th.join}
  end

  # This will launch up to 250 threads
  def close_sessions_concurrently2(session_list, num_sessions)
    session_list2 = session_list[0...num_sessions]
    threads = session_list2.map do |session|
      Thread.new do
        begin
          session.close
          session_list.delete(session)
        rescue Exception => e
          raise RuntimeError.new("Error while closing a session. #{e.class.name}: #{e.message}
                                  Backtrace: #{e.backtrace.inspect}")
        end
      end
    end

    threads.each {|th| th.join}
    session_list
  end

  def close_clusters_concurrently(cluster_list)
    threads = cluster_list.map do |cluster|
      Thread.new do
        begin
          cluster.close
        rescue Exception => e
          raise RuntimeError.new("Error while closing a cluster. #{e.class.name}: #{e.message}
                                  Backtrace: #{e.backtrace.inspect}")
        end
      end
    end

    threads.each {|th| th.join}
  end
end