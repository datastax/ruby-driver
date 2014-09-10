# encoding: utf-8

#--
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
#++

require 'fileutils'
require 'logger'

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

  if RUBY_ENGINE == 'jruby'
    class Runner
      def initialize(ccm_script, env, notifier)
        @cmd      = 'ccm'
        @env      = env
        @notifier = notifier
      end

      def exec(*args)
        cmd = args.dup.unshift(@cmd).join(' ')
        pid = nil
        out = ''

        IO.popen([@env, @cmd, *args]) do |io|
          pid = io.pid
          @notifier.executing_command(cmd, pid)

          loop do
            begin
              Timeout.timeout(30) do
                out << chunk = io.readpartial(4096)

                @notifier.command_output(pid, chunk)
              end
            rescue Timeout::Error
              @notifier.command_running(pid)
            rescue EOFError
              break
            end
          end
        end

        @notifier.executed_command(cmd, pid, $?)
        raise "#{cmd} failed" unless $?.success?

        out
      end
    end
  else
    class Runner
      def initialize(ccm_script, env, notifier)
        @ccm_script = ccm_script
        @env        = env
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
          if IO.select([@stdout], nil, nil, 30)
            begin
              chunk = @stdout.read_nonblock(4096)
              if chunk.end_with?('=== DONE ===')
                chunk.sub!('=== DONE ===', '')
                done = true
              end
              out << chunk
              @notifier.command_output(@pid, chunk) unless chunk.empty?
            rescue IO::WaitReadable
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
          else
            @notifier.command_running(@pid)
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
          @env,
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

    attr_reader :name

    def initialize(name, ccm, nodes_count = nil, datacenters = nil, keyspaces = nil)
      @name        = name
      @ccm         = ccm
      @datacenters = datacenters
      @keyspaces   = keyspaces

      @nodes = (1..nodes_count).map do |i|
        Node.new("node#{i}", 'DOWN', self)
      end if nodes_count
    end

    def running?
      nodes.any?(&:up?)
    end

    def stop
      return if nodes.all?(&:down?)

      if @cluster
        @cluster.close
        @cluster = @session = nil
      end

      @ccm.exec('stop')
      nodes.each(&:down!)

      nil
    end

    def start
      return if @cluster && nodes.all?(&:up?) && @cluster.hosts.select(&:up?).count == nodes.size

      if @cluster
        @cluster.close
        @cluster = @session = nil
      end

      attempts = 1

      begin
        @ccm.exec('start', '--wait-other-notice', '--wait-for-binary-proto')
      rescue
        @ccm.exec('stop') rescue nil
        %x{killall java}

        raise if attempts >= 10
        attempts += 1
        sleep(attempts * 0.4)
        retry
      end

      nodes.each(&:up!)

      options = {:logger => logger, :consistency => :all}

      if @username && @password
        options[:username] = @username
        options[:password] = @password
      end

      attempts = 1

      begin
        @cluster = Cassandra.connect(options)
      rescue
        raise if attempts >= 10
        attempts += 1
        sleep(attempts * 0.4)
        retry
      end

      sleep(1) until @cluster.hosts.all?(&:up?)

      @session = @cluster.connect

      nil
    end

    def restart
      stop
      start
    end

    def start_node(name)
      node = nodes.find {|n| n.name == name}
      return if node.nil? || node.up?

      attempts = 1

      begin
        @ccm.exec(node.name, 'start', '--wait-other-notice', '--wait-for-binary-proto')
      rescue
        @ccm.exec(node.name, 'stop') rescue nil

        raise if attempts >= 10
        attempts += 1
        sleep(attempts * 0.4)
        retry
      end

      node.up!

      if @cluster
        i  = name.sub('node', '')
        ip = "127.0.0.#{i}"
        sleep(1) until @cluster.has_host?(ip) && @cluster.host(ip).up?
      end

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
        @cluster.hosts.group_by(&:datacenter).size
      end
    end

    def nodes_count
      nodes.size
    end

    def enable_authentication
      stop
      @username = 'cassandra'
      @password = 'cassandra'
      @ccm.exec('updateconf', 'authenticator: PasswordAuthenticator')
      start

      [@username, @password]
    end

    def disable_authentication
      stop
      @ccm.exec('updateconf', 'authenticator: AllowAllAuthenticator')
      @username = @password = nil
      start
    end

    def setup_schema(schema)
      start

      @cluster.each_keyspace do |keyspace|
        next if keyspace.name.start_with?('system')

        @session.execute("DROP KEYSPACE #{keyspace.name}")
      end

      execute(schema)

      nil
    end

    def execute(cql)
      start

      cql.strip!
      cql.chomp!(";")
      cql.split(";\n").each do |statement|
        @session.execute(statement)
      end

      nil
    end

    def clear
      @ccm.exec('clear')
      nodes.each(&:down!)
      nil
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

    def logger
      @logger ||= begin
        log = Logger.new($stderr)
        log.level = Logger::INFO
        log
      end
    end
  end

  def cassandra_version
    ENV['CASSANDRA_VERSION'] || '2.0.10'
  end

  def cassandra_cluster
    'ruby-driver-cassandra-' + cassandra_version + '-test-cluster'
  end

  def setup_cluster(no_dc = 1, no_nodes_per_dc = 3)
    if cluster_exists?(cassandra_cluster)
      switch_cluster(cassandra_cluster)

      unless @current_cluster.nodes_count == (no_dc * no_nodes_per_dc) && @current_cluster.datacenters_count == no_dc
        @current_cluster.stop
        remove_cluster(@current_cluster.name)
        create_cluster(cassandra_cluster, cassandra_version, no_dc, no_nodes_per_dc)
      end
    else
      @current_cluster && @current_cluster.stop
      create_cluster(cassandra_cluster, cassandra_version, no_dc, no_nodes_per_dc)
    end

    @current_cluster.start
    @current_cluster
  end

  private

  def ccm
    @ccm ||= begin
      Runner.new(
        ccm_script,
        {
          'HOME'             => ccm_home,
          'MAX_HEAP_SIZE'    => '32M',
          'HEAP_NEWSIZE'     => '8M',
          'MALLOC_ARENA_MAX' => '1'
        },
        PrintingNotifier.new($stderr)
      )
    end
  end

  def ccm_home
    @ccm_home ||= begin
      ccm_home = File.expand_path(File.dirname(__FILE__) + '/../tmp')
      FileUtils.mkdir_p(ccm_home) unless File.directory?(ccm_home)
      ccm_home
    end
  end

  def ccm_script
    @ccm_script ||= File.expand_path(File.dirname(__FILE__) + '/ccm.py')
  end

  def switch_cluster(name)
    if @current_cluster
      if @current_cluster.name == name
        @current_cluster.start
        return nil
      end
      @current_cluster.stop
    end

    @current_cluster = clusters.find {|c| c.name == name}
    return unless @current_cluster

    ccm.exec('switch', @current_cluster.name)

    @current_cluster.start

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

    ccm.exec('create', '-n', nodes, '-v', 'binary:' + version, '-b', '-i', '127.0.0.', name)

    if ENV['TRAVIS'] == 'true'
      File.open(ccm_home + '/.ccm/' + name + '/cassandra.in.sh', 'w+') do |f|
        f.write(<<-SH)
  JVM_OPTS="$JVM_OPTS -XX:ThreadStackSize=8"
  JVM_OPTS="$JVM_OPTS -XX:InitiatingHeapOccupancyPercent=0"
  JVM_OPTS="$JVM_OPTS -XX:+AggressiveOpts"
  JVM_OPTS="$JVM_OPTS -XX:MaxPermSize=24m"
  JVM_OPTS="$JVM_OPTS -XX:NewRatio=2"
  JVM_OPTS="$JVM_OPTS -XX:NewRatio=2"
  JVM_OPTS="$JVM_OPTS -XX:MinHeapFreeRatio=5"
  JVM_OPTS="$JVM_OPTS -XX:MaxHeapFreeRatio=95"
  JVM_OPTS="$JVM_OPTS -XX:LargePageSizeInBytes=1m"
  JVM_OPTS="$JVM_OPTS -XX:+UseCompressedStrings"
  SH
      end
    end

    ccm.exec('updateconf', 'range_request_timeout_in_ms: 10000')
    ccm.exec('updateconf', 'read_request_timeout_in_ms: 10000')

    if cassandra_version.start_with?('1.2.')
      ccm.exec('updateconf', 'reduce_cache_sizes_at: 0')
      ccm.exec('updateconf', 'reduce_cache_capacity_to: 0')
      ccm.exec('updateconf', 'flush_largest_memtables_at: 0')
      ccm.exec('updateconf', 'index_interval: 512')
    else
      ccm.exec('updateconf', 'cas_contention_timeout_in_ms: 10000')
      ccm.exec('updateconf', 'file_cache_size_in_mb: 0')
    end

    ccm.exec('updateconf', 'truncate_request_timeout_in_ms: 10000')
    ccm.exec('updateconf', 'write_request_timeout_in_ms: 10000')
    ccm.exec('updateconf', 'write_request_timeout_in_ms: 10000')
    ccm.exec('updateconf', 'request_timeout_in_ms: 10000')
    ccm.exec('updateconf', 'native_transport_max_threads: 1')
    ccm.exec('updateconf', 'rpc_min_threads: 1')
    ccm.exec('updateconf', 'rpc_max_threads: 1')
    ccm.exec('updateconf', 'concurrent_reads: 2')
    ccm.exec('updateconf', 'concurrent_writes: 2')
    ccm.exec('updateconf', 'concurrent_compactors: 1')
    ccm.exec('updateconf', 'compaction_throughput_mb_per_sec: 0')
    ccm.exec('updateconf', 'in_memory_compaction_limit_in_mb: 1')
    ccm.exec('updateconf', 'key_cache_size_in_mb: 0')
    ccm.exec('updateconf', 'key_cache_save_period: 0')
    ccm.exec('updateconf', 'memtable_flush_writers: 1')
    ccm.exec('updateconf', 'max_hints_delivery_threads: 1')

    clusters << @current_cluster = Cluster.new(name, ccm, nodes_per_datacenter * datacenters, datacenters, [])

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
