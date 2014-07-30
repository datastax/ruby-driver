# encoding: utf-8

require 'aruba/cucumber'
require 'pathname'
require 'fileutils'
require 'tempfile'
require 'yaml'

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

  class Runner
    def initialize(cmd, notifier)
      @cmd      = cmd
      @notifier = notifier
    end

    def exec(*args)
      cmd = args.unshift(@cmd).join(' ')

      @notifier.executing_command(cmd)

      out = `#{cmd}`

      @notifier.executed_command(cmd, out, $?)

      raise "#{cmd} failed" unless $?.success?

      out
    end
  end

  class Cluster
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
      return if keyspaces.include?(keyspace)

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
      sleep(4)

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
      File.read(fixture_path + 'keyspace' + "#{table}.cql")
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
    'cassandra-2.0.7'
  end

  def cassandra_cluster
    'test-cluster'
  end

  def ccm
    @ccm ||= Runner.new('ccm', PrintingNotifier.new($stderr))
  end


  # create new ccm cluster from a given cassandra tag
  def create_cluster(cluster, version, no_dc, no_nodes_per_dc)
    version = "git:#{version}"
    nodes = Array.new(no_dc, no_nodes_per_dc).join(":")

    ccm.exec('create', '-n', nodes, '-v', version, '-b', '-i 127.0.0.', cluster)
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

    # @prev_cluster = current_cluster
    # if @prev_cluster == name
    #   @prev_cluster = nil
    # else
    #   stop_cluster if @prev_cluster
    #   switch_cluster(name)
    # end

    cluster
  end

  def create_if_necessary(name, no_dc, no_nodes_per_dc)
    if Cluster.exists?(name, ccm)
      switch_cluster(name) unless name == current_cluster
      cluster = Cluster.new(name, ccm)
      if cluster.running? and cluster.has_n_datacenters?(no_dc) and cluster.has_n_nodes_per_dc?(no_nodes_per_dc)
        cluster.start_down_nodes
      else
        remove_cluster(name)
        create_cluster(name, cassandra_version, no_dc, no_nodes_per_dc)
        Cluster.new(name, ccm).start
      end
    else
      create_cluster(name, cassandra_version, no_dc, no_nodes_per_dc)
      Cluster.new(name, ccm).start
    end
  end
end

World(CCM)

Before do
  @aruba_timeout_seconds = 15
end

After do |s| 
  # Tell Cucumber to quit after this scenario is done - if it failed.
  Cucumber.wants_to_quit = true if s.failed? and ENV["FAIL_FAST"] == 'Y'
end

unless ENV['COVERAGE'] == 'no' || RUBY_ENGINE == 'rbx'
  require 'coveralls'
  require 'simplecov'

  if ENV.include?('TRAVIS')
    Coveralls.wear!
    SimpleCov.formatter = Coveralls::SimpleCov::Formatter
  end

  SimpleCov.start do
    add_group 'Source', 'lib'
    add_group 'Unit tests', 'spec/cql'
    add_group 'Integration tests', 'spec/integration'
    add_group 'Features', 'features'
  end
end
