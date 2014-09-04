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

require 'fileutils'

# Cassandra Cluster Manager integration for
# driving a cassandra cluster from tests.
module CCM extend self
  class PrintingNotifier
    def initialize(out)
      @out = out
    end

    def executing_command(cmd, pid)
      @out << "$> #{cmd} (#{pid})\n"
    end

    def command_output(pid, chunk)
      @out << chunk
    end

    def command_running(pid)
      @out << "\n....still running....\n"
    end

    def executed_command(cmd, pid, status)
      @out << "   [exit=#{status.exitstatus}]\n"
    end
  end

  class NullNotifier
    def executing_command(cmd, pid)
    end

    def command_output(pid, chunk)
    end

    def command_running(pid)
    end

    def executed_command(cmd, pid, status)
    end
  end

  class Runner
    def initialize(ccm_home, ccm_script, notifier)
      @ccm_home   = ccm_home
      @ccm_script = ccm_script
      @notifier   = notifier
    end

    def exec(*args)
      start_ccm_helper

      cmd = args.dup.unshift('ccm').join(' ')
      out = ''
      done = false

      @notifier.executing_command(cmd, @pid)

      @stdin.write(encode(args))
      until done
        begin
          Timeout.timeout(30) do
            chunk = @stdout.readpartial(4096)
            if chunk.end_with?('=== DONE ===')
              chunk.sub!('=== DONE ===', '')
              done = true
            end
            out << chunk
            @notifier.command_output(@pid, chunk) unless chunk.empty?
          end
        rescue Timeout::Error
          @notifier.command_running(@pid)
        rescue EOFError
          @stdin.close
          @stdout.close
          Process.waitpid(@pid)

          @notifier.executed_command(cmd, @pid, $?)
          @stdin  = nil
          @stdout = nil
          @pid    = nil

          raise "#{cmd} failed"
        end
      end

      out
    end

    private

    def start_ccm_helper
      return if @stdin && @stdout && @pid

      in_r, @stdin = IO.pipe
      @stdout, out_w = IO.pipe

      @stdin.sync = true

      @pid = Process.spawn(
        {
          'HOME' => @ccm_home
        },
        'python', '-u', @ccm_script,
        {
          :in => in_r,
          [:out, :err] => out_w
        }
      )

      in_r.close
      out_w.close
    end

    def encode(args)
      body = JSON.dump(args)
      size = body.bytesize
      [size, body].pack("S!A#{size}")
    end
  end

  class Cluster
    class Node
      attr_reader :name, :status

      def initialize(name, status, cluster)
        @name    = name
        @status  = status
        @cluster = cluster
      end

      def stop
        return if @status == 'DOWN'
        @cluster.stop_node(@name)
        @status = 'DOWN'
        nil
      end

      def start
        return if @status == 'UP'
        @cluster.start_node(@name)
        @status = 'UP'
        nil
      end

      def decommission
        @cluster.decommission_node(@name)
        nil
      end

      def remove
        stop
        @cluster.remove_node(@name)
        nil
      end

      def up!
        @status = 'UP'
        nil
      end

      def down!
        @status = 'DOWN'
        nil
      end

      def up?
        @status == 'UP'
      end

      def down?
        @status == 'DOWN'
      end
    end

    class Keyspace
      attr_reader :name

      def initialize(name, cluster)
        @name    = name
        @cluster = cluster
      end

      def drop
        @cluster.drop_keyspace(@name)
      end

      def tables
        @tables ||= begin
          data = @cluster.execute_query("USE #{@name}; DESCRIBE TABLES")
          data.strip!

          if data == "<empty>"
            []
          else
            data.split(/\s+/).map! do |name|
              Table.new(name, @name, @cluster)
            end
          end
        end
      end

      def table(name)
        table = tables.find {|t| t.name == name}

        return table if table

        @cluster.create_table(@name, name)
        tables << table = Table.new(name, @name, @cluster)
        table
      end
    end

    class Table
      attr_reader :name, :keyspace

      def initialize(name, keyspace, cluster)
        @name     = name
        @keyspace = keyspace
        @cluster  = cluster
      end

      def load
        @cluster.populate_table(@keyspace, @name)
      end

      def clear
        @cluster.clear_table(@keyspace, @name)
      end
    end

    attr_reader :name

    def initialize(name, ccm, nodes_count = nil, datacenters = nil, keyspaces = nil)
      @name        = name
      @ccm         = ccm
      @datacenters = datacenters
      @keyspaces   = keyspaces

      @nodes = (1..nodes_count).map do |i|
        Node.new("node#{i}", 'UP', self)
      end if nodes_count
    end

    def running?
      nodes.any?(&:up?)
    end

    def stop
      return if nodes.all?(&:down?)

      @ccm.exec('stop')
      @nodes.each(&:down!)

      nil
    end

    def start
      return if nodes.all?(&:up?)

      @ccm.exec('start', '--wait-for-binary-proto')
      @nodes.each(&:up!)

      nil
    end

    def restart
      stop
      start
    end

    def start_node(name)
      node = nodes.find {|n| n.name == name}
      return if node.nil? || node.up?
      @ccm.exec(node.name, 'start', '--wait-other-notice', '--wait-for-binary-proto')
      node.up!

      nil
    end

    def stop_node(name)
      node = nodes.find {|n| n.name == name}
      return if node.nil? || node.down?
      @ccm.exec(node.name, 'stop')
      node.down!

      nil
    end

    def remove_node(name)
      node = nodes.find {|n| n.name == name}
      return if node.nil?
      @ccm.exec(node.name, 'stop')
      @ccm.exec(node.name, 'remove')
      node.down!
      nodes.delete(node)

      nil
    end

    def decommission_node(name)
      node = nodes.find {|n| n.name == name}
      return if node.nil?
      @ccm.exec(node.name, 'decommission')

      nil
    end

    def add_node(name)
      return if nodes.any? {|n| n.name == name}

      i = name.sub('node', '')

      @ccm.exec('add', '-b', "-t 127.0.0.#{i}:9160", "-l 127.0.0.#{i}:7000", "--binary-itf=127.0.0.#{i}:9042", name)
      nodes << Node.new(name, 'DOWN', self)

      nil
    end

    def datacenters_count
      @datacenters ||= begin
        start if !running?
        node = nodes.find(&:up?)
        @ccm.exec(node.name, 'status')
            .split("\n")
            .find_all { |line| line.start_with? "Datacenter:" }
            .count
      end
    end

    def nodes_count
      nodes.size
    end

    def keyspace(name)
      keyspace = keyspaces.find {|k| k.name == name}
      return keyspace if keyspace

      execute_query("CREATE KEYSPACE #{name} WITH replication = " \
                    "{'class': 'SimpleStrategy', 'replication_factor': 3}")
      sleep(2)

      keyspaces << keyspace = Keyspace.new(name, self)
      keyspace
    end

    def drop_keyspace(name)
      keyspace = keyspaces.find {|k| k.name == name}
      return if keyspace.nil?
      execute_query("DROP KEYSPACE #{keyspace.name}")
      keyspaces.delete(keyspace)

      nil
    end

    def create_table(keyspace, table)
      execute_query("USE #{keyspace}; DROP TABLE IF EXISTS #{table}; " + schema_for(table).chomp(";\n"))
    end

    def populate_table(keyspace, table)
      execute_query("USE #{keyspace}; " + data_for(table).chomp(";\n"))
    end

    def clear_table(keyspace, table)
      execute_query("USE #{keyspace}; TRUNCATE #{table}")
    end

    def execute_query(query)
      start if !running?
      node = nodes.find(&:up?)

      # for some reason cqlsh -x it eating first 4 lines of output, so we make it output 4 lines of version first
      prefix  = 'show version; ' * 4

      @ccm.exec(node.name, 'cqlsh', '-v', '-x', "#{prefix}#{query}")
    end

    def enable_authentication
      @username = 'cassandra'
      @password = 'cassandra'
      stop
      @ccm.exec('updateconf', 'authenticator: PasswordAuthenticator')
      start

      sleep(2)

      [@username, @password]
    end

    def disable_authentication
      stop
      @ccm.exec('updateconf', 'authenticator: AllowAllAuthenticator')
      start
    end

    def clear_schema
      @keyspaces = nil
    end

    private

    def nodes
      @nodes ||= begin
        @ccm.exec('status').split("\n").map! do |line|
          line.strip!
          name, status = line.split(": ")
          Node.new(name, status, self)
        end
      end
    end

    # path to cql fixture files
    def fixture_path
      @fixture_path ||= Pathname(File.dirname(__FILE__) + '/cql')
    end

    def schema_for(table)
      File.read(fixture_path + 'schema' + "#{table}.cql")
    end

    def data_for(table)
      File.read(fixture_path + 'data' + "#{table}.cql")
    end

    def keyspaces
      @keyspaces ||= begin
        data = execute_query("DESCRIBE KEYSPACES")
        data.strip!
        data.split(/\s+/).map! do |name|
          Keyspace.new(name, self)
        end
      end
    end
  end

  def cassandra_version
    ENV['CASSANDRA_VERSION'] || '2.0.9'
  end

  def cassandra_cluster
    'ruby-driver-cassandra-' + cassandra_version + '-test-cluster'
  end

  def setup_cluster(no_dc = 1, no_nodes_per_dc = 3, attempts = 1)
    if cluster_exists?(cassandra_cluster)
      switch_cluster(cassandra_cluster)

      if @current_cluster.nodes_count == (no_dc * no_nodes_per_dc) && @current_cluster.datacenters_count == no_dc
        @current_cluster.start
      else
        @current_cluster.stop
        remove_cluster(@current_cluster.name)
        create_cluster(cassandra_cluster, cassandra_version, no_dc, no_nodes_per_dc)
      end
    else
      @current_cluster && @current_cluster.stop
      create_cluster(cassandra_cluster, cassandra_version, no_dc, no_nodes_per_dc)
    end

    @current_cluster
  rescue
    clear
    ccm.exec('stop') rescue nil
    raise if attempts == 3
    attempts += 1
    retry
  end

  private

  def ccm
    @ccm ||= begin
      ccm_home = File.expand_path(File.dirname(__FILE__) + '/../tmp')
      FileUtils.mkdir_p(ccm_home) unless File.directory?(ccm_home)
      ccm_script = File.expand_path(File.dirname(__FILE__) + '/ccm.py')
      Runner.new(ccm_home, ccm_script, PrintingNotifier.new($stderr))
    end
  end

  def switch_cluster(name)
    if @current_cluster
      return if @current_cluster.name == name
      @current_cluster.stop
    end

    @current_cluster = clusters.find {|c| c.name == name}
    return unless @current_cluster

    ccm.exec('switch', @current_cluster.name)

    nil
  end

  def remove_cluster(name)
    cluster = clusters.find {|c| c.name == name}
    return unless cluster
    ccm.exec('remove', cluster.name)
    clusters.delete(cluster)

    nil
  end

  def create_cluster(name, version, datacenters, nodes_per_datacenter)
    nodes = Array.new(datacenters, nodes_per_datacenter).join(':')

    ccm.exec('create', '-n', nodes, '-v', 'binary:' + version, '-b', '-s', '-i', '127.0.0.', name)

    @current_cluster = cluster = Cluster.new(name, ccm, nodes_per_datacenter * datacenters, datacenters, [])

    clusters << cluster

    nil
  end

  def update_conf
    ccm.exec('updateconf')
    nil
  end

  def clusters
    @clusters ||= begin
      ccm.exec('list').split("\n").map! do |name|
        name.strip!
        current = name.start_with?('*')
        name.sub!('*', '')
        cluster = Cluster.new(name, ccm)
        @current_cluster = cluster if current
        cluster
      end
    end
  end

  def cluster_exists?(name)
    clusters.any? {|cluster| cluster.name == name}
  end

  def clear
    instance_variables.each do |ivar|
      remove_instance_variable(ivar)
    end

    nil
  end
end
