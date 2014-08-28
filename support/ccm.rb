# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
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

# Cassandra Cluster Manager integration for
# driving a cassandra cluster from tests.
module CCM
  class PrintingNotifier
    def initialize(out)
      @out = out
    end

    def executing_command(cmd)
      @out << "$> #{cmd}\n"
    end

    def executed_command(cmd, out, status)
      out.split("\n").each do |line|
        @out << "      #{line}\n"
      end
      @out << "   [exit=#{status.exitstatus}]\n"
    end
  end

  class NullNotifier
    def executing_command(cmd)
    end

    def executed_command(cmd, out, status)
    end
  end

  class Runner
    def initialize(cmd, notifier)
      @cmd      = cmd
      @notifier = notifier
    end

    def exec(*args)
      cmd = args.unshift(@cmd).join(' ')

      @notifier.executing_command(cmd)

      out = `#{cmd} 2>&1`

      @notifier.executed_command(cmd, out, $?)
      raise "#{cmd} failed" unless $?.success?

      out
    end
  end

  class Cluster
    def self.stop_existing_cluster(ccm)
      if (ccm.exec('list') =~ /\*/) != nil
        ccm.exec('stop')
      end
    end

    def self.exists?(cluster, ccm)
      ccm.exec('list').split("\n").map(&:strip).one? do |name|
        name == cluster || name == "*#{cluster}"
      end
    end

    def self.list_nodes(ccm)
      ccm.exec('status').split("\n").map do |line|
        node, _ = line.split(": ")
        node
      end
    end

    def self.count_datacenters(node, ccm)
      ccm.
          exec("#{node} status").
          split("\n").
          find_all { |line| line.start_with? "Datacenter:" }.
          count
    end

    # initialize could just take name, cmm and find out the nodes by itself

    def initialize(name, ccm)
      @name  = name
      @ccm   = ccm

      start unless running?

      @nodes = Cluster.list_nodes(ccm)
      @no_dc = Cluster.count_datacenters(@nodes.first, ccm)
    end

    def has_n_datacenters?(n)
      n == @no_dc
    end

    def has_n_nodes_per_dc?(n)
      # Could be improved : assume that the nodes are evenly distributed amongst datacenters
      n == (@nodes.count / @no_dc)
    end

    def create_keyspace(keyspace)
      if keyspaces.include?(keyspace)
        drop_keyspace(keyspace)
      end

      execute_query("CREATE KEYSPACE #{keyspace} WITH replication = " \
                    "{'class': 'SimpleStrategy', 'replication_factor': 3}")
    end

    def use_keyspace(keyspace)
      @keyspace = keyspace
    end

    def drop_keyspace(keyspace)
      execute_query("DROP KEYSPACE #{keyspace}")
    end

    def create_table(table)
      raise "no keyspace selected" if @keyspace.nil?

      execute_query("USE #{@keyspace}; DROP TABLE IF EXISTS #{table}; " +
                    keyspace_for(table).chomp(";\n"))
    end

    def populate_table(table)
      execute_query("USE #{@keyspace}; " + data_for(table).chomp(";\n"))
    end

    def start_nth_node(i)
      start_node("node#{i}")
    end

    def start_node(node)
      @ccm.exec(node, 'start')
    end

    def stop_node(i)
      @ccm.exec("node#{i}", 'stop')
    end

    def add_node(i)
      @ccm.exec('add', '-b', "-t 127.0.0.#{i}:9160", "-l 127.0.0.#{i}:7000", "--binary-itf=127.0.0.#{i}:9042", "node#{i}")
    end

    def decommission_node(i)
      @ccm.exec("node#{i}", 'decommission')
    end

    def remove_node(i)
      @ccm.exec("node#{i}", 'remove')
    end

    def enable_authentication
      @username = 'cassandra'
      @password = 'cassandra'
      @ccm.exec('updateconf', "'authenticator: PasswordAuthenticator'")
      restart
      sleep(10)

      [@username, @password]
    end

    def disable_authentication
      @ccm.exec('updateconf', "'authenticator: AllowAllAuthenticator'")
    end

    def restart
      stop
      start
    end

    def clear
      @ccm.exec('clear')
      self
    end

    def start
      @ccm.exec('start')
      self
    end

    def stop
      @ccm.exec('stop')
      self
    end

    def start_down_nodes
      @ccm.exec('status').
          split("\n").
          find_all { |line| line.end_with? "DOWN" }.
          map { |line| line.sub(': DOWN', '') }.
          each { |node| start_node(node) }
      self
    end

    def running?
      @ccm.exec('status').
        split("\n").
        find_all { |line| line.end_with? "DOWN" }.
        none?
    end

    private

    # path to cql fixture files
    def fixture_path
      @fixture_path ||= Pathname(File.dirname(__FILE__) + '/cql')
    end

    def keyspace_for(table)
      File.read(fixture_path + 'schema' + "#{table}.cql")
    end

    def data_for(table)
      File.read(fixture_path + 'data' + "#{table}.cql")
    end

    def keyspaces
      execute_query("DESCRIBE KEYSPACES").strip.split(/\s+/)
    end

    def tables
      data = execute_query("USE #{@keyspace}; DESCRIBE TABLES").strip
      return [] if data == "<empty>"
      data.split(/\s+/)
    end

    def any_node
      @nodes.sample
    end

    def execute_query(query)
      # for some reason cqlsh -x it eating first 4 lines of output, so we make it output 4 lines of version first
      prefix  = 'show version; ' * 4

      @ccm.exec(any_node, 'cqlsh', '-v', '-x', "\"#{prefix}#{query}\"")
    end
  end

  def cassandra_version
    ENV['CASSANDRA_VERSION'] || '2.0.9'
  end

  def cassandra_cluster
    'ruby-driver-cassandra-' + cassandra_version + '-test-cluster'
  end

  def ccm
    @ccm ||= Runner.new('ccm', PrintingNotifier.new($stderr))
  end


  # create new ccm cluster from a given cassandra tag
  def create_cluster(cluster, version, no_dc, no_nodes_per_dc)
    nodes = Array.new(no_dc, no_nodes_per_dc).join(":")

    ccm.exec('create', '-n', nodes, '-v', version, '-b', '-s', '-i', '127.0.0.', cluster)
    @current_no_dc=no_dc
    @current_no_nodes_per_dc=no_nodes_per_dc
    nil
  end

  def update_conf
    ccm.exec('updateconf')
    nil
  end

  def current_cluster
    current = ccm.exec('list') \
                 .split("\n")  \
                 .map(&:strip) \
                 .find {|l| l.start_with?("*")}

    return if current.nil?

    current[1..-1]
  end

  def switch_cluster(cluster)
    ccm.exec('switch', cluster)
    nil
  end

  def remove_cluster(cluster)
    ccm.exec('remove', cluster)
    nil
  end

  def setup_cluster(no_dc = 1, no_nodes_per_dc = 3)
    name = cassandra_cluster
    cluster = create_if_necessary(name, no_dc, no_nodes_per_dc)
  end

  def create_if_necessary(name, no_dc, no_nodes_per_dc)
    if Cluster.exists?(name, ccm)
      if name != current_cluster
        Cluster.stop_existing_cluster(ccm)
        switch_cluster(name)
      end

      cluster = Cluster.new(name, ccm)
      if cluster.running? and cluster.has_n_datacenters?(no_dc) and cluster.has_n_nodes_per_dc?(no_nodes_per_dc)
        cluster.start_down_nodes
      else
        remove_cluster(name)
        create_cluster(name, cassandra_version, no_dc, no_nodes_per_dc)
        Cluster.new(name, ccm).start
      end
    else
      # stops any existing cluster so we can re-bind localhost
      Cluster.stop_existing_cluster(ccm)
      create_cluster(name, cassandra_version, no_dc, no_nodes_per_dc)
      Cluster.new(name, ccm).start
    end
  end
end
