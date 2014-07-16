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
    def initialize(name, ccm, nodes)
      @name  = name
      @ccm   = ccm
      @nodes = nodes
    end

    def create_schema(schema)
      return if schemas.include?(schema)

      execute_query("CREATE KEYSPACE #{schema} WITH replication = " \
                    "{'class': 'SimpleStrategy', 'replication_factor': 3}")
    end

    def use_schema(schema)
      @schema = schema
    end

    def drop_schema(schema)
      execute_query("DROP KEYSPACE #{schema}")
    end

    def create_table(table)
      raise "no schema selected" if @schema.nil?

      execute_query("USE #{@schema}; DROP TABLE #{table}; " +
                    schema_for(table).chomp(";\n"))
    end

    def populate_table(table)
      execute_query("USE #{@schema}; " + data_for(table).chomp(";\n"))
    end

    def start_node(i)
      @ccm.exec("node#{i}", 'start')
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

    def setup_authentication
      @ccm.exec('stop')
      set_authenticator_to('PasswordAuthenticator')
      @authentication = true
      @username = 'cassandra'
      @password = 'cassandra'
      @ccm.exec('start')

      [@username, @password]
    end

    private

    def set_authenticator_to(authenticator)
      @nodes.each do |node|
        config = load_config(node)
        next if config['authenticator'] == authenticator
        config.merge!({'authenticator'  => authenticator})
        save_config(node, config)
      end
    end

    def ccm_dir
      @ccm_dir ||= Pathname('~/.ccm')
    end

    def cassandra_config_path(node)
      (ccm_dir + @name + node + 'conf/cassandra.yaml').expand_path
    end

    def load_config(node)
      YAML.load(File.read(cassandra_config_path(node)))
    end

    def save_config(node, config)
      Tempfile.open('cassanrda.yaml') do |f|
        f.write(YAML.dump(config))
        f.flush
        FileUtils.mv(f.path, cassandra_config_path(node))
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

    def schemas
      execute_query("DESCRIBE KEYSPACES").strip.split(/\s+/)
    end

    def tables
      data = execute_query("USE #{@schema}; DESCRIBE TABLES").strip
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

  # check if ccm tool already has a test-cluster
  def cluster_exists?(cluster)
    ccm.exec('list').split("\n").map(&:strip).one? do |name|
      name == cluster || name == "*#{cluster}"
    end
  end

  # create new ccm cluster from a given cassandra tag
  def create_cluster(cluster, version)
    return if cluster_exists?(cassandra_cluster)

    version = "git:#{version}"

    ccm.exec('create', '-n', 3, '-v', version, '-b', cluster)
    nil
  end

  def start_cluster
    ccm.exec('updateconf')
    ccm.exec('start')
    nil
  end

  def stop_cluster
    ccm.exec('stop')
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

  def cluster_nodes
    ccm.exec('status').split("\n").map do |line|
      node, _ = line.split(": ")
      node
    end
  end

  def setup_cluster
    create_cluster(cassandra_cluster, cassandra_version)

    @prev_cluster = current_cluster

    if @prev_cluster == cassandra_cluster
      @prev_cluster = nil
    else
      stop_cluster if @prev_cluster
      switch_cluster(cassandra_cluster)
    end

    stop_cluster
    start_cluster

    Cluster.new(cassandra_cluster, ccm, cluster_nodes)
  end

  def teardown_cluster
    return unless @prev_cluster
    stop_cluster
    switch_cluster(@prev_cluster)
    @prev_cluster = nil
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
