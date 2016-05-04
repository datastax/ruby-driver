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

require 'ione'
require 'json'

require 'monitor'
require 'ipaddr'
require 'set'
require 'bigdecimal'
require 'forwardable'
require 'digest'
require 'stringio'
require 'resolv'
require 'openssl'
require 'securerandom'
require 'time'
require 'date'

module Cassandra
  # A list of all supported request consistencies
  # @see http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html Consistency
  #   levels in Apache Cassandra 2.0
  # @see http://www.datastax.com/documentation/cassandra/1.2/cassandra/dml/dml_config_consistency_c.html Consistency
  #   levels in Apache Cassandra 1.2
  # @see Cassandra::Session#execute_async
  CONSISTENCIES = [:any, :one, :two, :three, :quorum, :all, :local_quorum,
                   :each_quorum, :serial, :local_serial, :local_one].freeze

  # A list of all supported serial consistencies
  # @see Cassandra::Session#execute_async
  SERIAL_CONSISTENCIES = [:serial, :local_serial].freeze

  # A list of all possible write types that a
  # {Cassandra::Errors::WriteTimeoutError} can have.
  #
  # @see https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v2.spec#L872-L887 Description of
  #   possible types of writes in Apache Cassandra native protocol spec v1
  WRITE_TYPES = [:simple, :batch, :unlogged_batch, :counter, :batch_log].freeze

  CLUSTER_OPTIONS = [
    :address_resolution,
    :address_resolution_policy,
    :auth_provider,
    :client_cert,
    :client_timestamps,
    :compression,
    :compressor,
    :connect_timeout,
    :connections_per_local_node,
    :connections_per_remote_node,
    :consistency,
    :credentials,
    :datacenter,
    :futures_factory,
    :heartbeat_interval,
    :hosts,
    :idle_timeout,
    :listeners,
    :load_balancing_policy,
    :logger,
    :nodelay,
    :reconnection_policy,
    :retry_policy,
    :page_size,
    :passphrase,
    :password,
    :port,
    :private_key,
    :protocol_version,
    :requests_per_connection,
    :schema_refresh_delay,
    :schema_refresh_timeout,
    :server_cert,
    :shuffle_replicas,
    :ssl,
    :synchronize_schema,
    :timeout,
    :trace,
    :username
  ].freeze

  # Creates a {Cassandra::Cluster Cluster instance}.
  #
  # @option options [Array<String, IPAddr>] :hosts (['127.0.0.1']) a list of
  #   initial addresses. Note that the entire list of cluster members will be
  #   discovered automatically once a connection to any hosts from the original
  #   list is successful.
  #
  # @option options [Integer] :port (9042) cassandra native protocol port.
  #
  # @option options [Boolean] :nodelay (true) when set to `true`, disables
  #   nagle algorithm.
  #
  # @option options [String] :datacenter (nil) name of current datacenter.
  #   First datacenter found will be assumed current by default. Note that you
  #   can skip this option if you specify only hosts from the local datacenter
  #   in `:hosts` option.
  #
  # @option options [Boolean] :shuffle_replicas (true) whether replicas list
  #   found by the default Token-Aware Load Balancing Policy should be
  #   shuffled. See {Cassandra::LoadBalancing::Policies::TokenAware#initialize Token-Aware Load Balancing Policy}.
  #
  # @option options [Numeric] :connect_timeout (10) connection timeout in
  #   seconds. Setting value to `nil` will reset it to 5 seconds.
  #
  # @option options [Numeric] :timeout (10) request execution timeout in
  #   seconds. Setting value to `nil` will remove request timeout.
  #
  # @option options [Numeric] :heartbeat_interval (30) how often should a
  #   heartbeat be sent to determine if a connection is alive. Several things to
  #   note about this option. Only one heartbeat request will ever be
  #   outstanding on a given connection. Each heatbeat will be sent in at least
  #   `:heartbeat_interval` seconds after the last request has been sent on a
  #   given connection. Setting value to `nil` will remove connection timeout.
  #
  # @option options [Numeric] :idle_timeout (60) period of inactivity after
  #   which a connection is considered dead. Note that this value should be at
  #   least a few times larger than `:heartbeat_interval`. Setting value to
  #   `nil` will remove automatic connection termination.
  #
  # @option options [String] :username (none) username to use for
  #   authentication to cassandra. Note that you must also specify `:password`.
  #
  # @option options [String] :password (none) password to use for
  #   authentication to cassandra. Note that you must also specify `:username`.
  #
  # @option options [Boolean, OpenSSL::SSL::SSLContext] :ssl (false) enable
  #   default ssl authentication if `true` (not recommended). Also accepts an
  #   initialized {OpenSSL::SSL::SSLContext}. Note that this option should be
  #   ignored if `:server_cert`, `:client_cert`, `:private_key` or
  #   `:passphrase` are given.
  #
  # @option options [String] :server_cert (none) path to server certificate or
  #   certificate authority file.
  #
  # @option options [String] :client_cert (none) path to client certificate
  #   file. Note that this option is only required when encryption is
  #   configured to require client authentication.
  #
  # @option options [String] :private_key (none) path to client private key.
  #   Note that this option is only required when encryption is configured to
  #   require client authentication.
  #
  # @option options [String] :passphrase (none) passphrase for private key.
  #
  # @option options [Symbol] :compression (none) compression to use. Must be
  #   either `:snappy` or `:lz4`. Also note, that in order for compression to
  #   work, you must install 'snappy' or 'lz4-ruby' gems.
  #
  # @option options [Cassandra::LoadBalancing::Policy] :load_balancing_policy
  #   default: token aware data center aware round robin.
  #
  # @option options [Symbol] :address_resolution (:none) a pre-configured
  #   address resolver to use. Must be one of `:none` or
  #   `:ec2_multi_region`.
  #
  # @option options [Integer] :connections_per_local_node (nil) Number of connections to
  #   open to each local node; the value of this option directly correlates to the number
  #   of requests the client can make to the local node concurrently. When `nil`, the
  #   setting is `1` for nodes that use the v3 or later protocol, and `2` for nodes that
  #   use the v2 or earlier protocol.
  #
  # @option options [Integer] :connections_per_remote_node (1) Number of connections to
  #   open to each remote node; the value of this option directly correlates to the
  #   number of requests the client can make to the remote node concurrently.
  #
  # @option options [Integer] :requests_per_connection (nil) Number of outstanding
  #   requests to support on one connection. Depending on the types of requests, some may
  #   get processed in parallel in the Cassandra node. When `nil`, the setting is `1024`
  #   for nodes that use the v3 or later protocol, and `128` for nodes that use the
  #   v2 or earlier protocol.
  #
  # @option options [Integer] :protocol_version (nil) Version of protocol to speak to
  #   nodes. By default, this is auto-negotiated to the lowest common protocol version
  #   that all nodes in `:hosts` speak.
  #
  # @option options [Boolean, Cassandra::TimestampGenerator] :client_timestamps (false) whether the driver
  #   should send timestamps for each executed statement and possibly which timestamp generator to use. Enabling this
  #   setting helps mitigate Cassandra cluster clock skew because the timestamp of the client machine will be used.
  #   This does not help mitigate application cluster clock skew. Also accepts an initialized
  #   {Cassandra::TimestampGenerator}, `:simple` (indicating an instance of {Cassandra::TimestampGenerator::Simple}),
  #   or `:monotonic` (indicating an instance of {Cassandra::TimestampGenerator::TickingOnDuplicate}). If set to true,
  #   it defaults to {Cassandra::TimestampGenerator::Simple} for all Ruby flavors except JRuby. On JRuby, it defaults to
  #   {Cassandra::TimestampGenerator::TickingOnDuplicate}.
  #
  # @option options [Boolean] :synchronize_schema (true) whether the driver
  #   should automatically keep schema metadata synchronized. When enabled, the
  #   driver updates schema metadata after receiving schema change
  #   notifications from Cassandra. Setting this setting to `false` disables
  #   automatic schema updates. Schema metadata is used by the driver to
  #   determine cluster partitioners as well as to find partition keys and
  #   replicas of prepared statements, this information makes token aware load
  #   balancing possible. One can still
  #   {Cassandra::Cluster#refresh_schema refresh schema manually}.
  #
  # @option options [Numeric] :schema_refresh_delay (1) the driver will wait
  #   for `:schema_refresh_delay` before fetching metadata after receiving a
  #   schema change event. This timer is restarted every time a new schema
  #   change event is received. Finally, when the timer expires or a maximum
  #   wait time of `:schema_refresh_timeout` has been reached, a schema refresh
  #   attempt will be made and the timeout is reset.
  #
  # @option options [Numeric] :schema_refresh_timeout (10) the maximum delay
  #   before automatically refreshing schema. Such delay can occur whenever
  #   multiple schema change events are continuously arriving within
  #   `:schema_refresh_delay` interval.
  #
  # @option options [Cassandra::Reconnection::Policy] :reconnection_policy
  #   default: {Cassandra::Reconnection::Policies::Exponential Exponential}.
  #   Note that the default policy is configured with `(0.5, 30, 2)`.
  #
  # @option options [Cassandra::Retry::Policy] :retry_policy default:
  #   {Cassandra::Retry::Policies::Default Default Retry Policy}.
  #
  # @option options [Logger] :logger (none) logger. a {Logger} instance from the
  #   standard library or any object responding to standard log methods
  #   (`#debug`, `#info`, `#warn`, `#error` and `#fatal`).
  #
  # @option options [Enumerable<Cassandra::Listener>] :listeners (none)
  #   initial listeners. A list of initial cluster state listeners. Note that a
  #   `:load_balancing` policy is automatically registered with the cluster.
  #
  # @option options [Symbol] :consistency (:local_one) default consistency
  #   to use for all requests. Must be one of {Cassandra::CONSISTENCIES}.
  #
  # @option options [Boolean] :trace (false) whether or not to trace all
  #   requests by default.
  #
  # @option options [Integer] :page_size (10000) default page size for all
  #   select queries. Set this value to `nil` to disable paging.
  #
  # @option options [Hash{String => String}] :credentials (none) a hash of credentials -
  #   to be used with [credentials authentication in cassandra 1.2](https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v1.spec#L238-L250).
  #   Note that if you specified `:username` and `:password` options, those credentials
  #   are configured automatically.
  #
  # @option options [Cassandra::Auth::Provider] :auth_provider (none) a custom auth
  #   provider to be used with [SASL authentication in cassandra 2.0](https://github.com/apache/cassandra/blob/cassandra-2.0.16/doc/native_protocol_v2.spec#L257-L273).
  #   Note that if you have specified `:username` and `:password`, then a
  #   {Cassandra::Auth::Providers::Password Password Provider} will be used automatically.
  #
  # @option options [Cassandra::Compression::Compressor] :compressor (none) a
  #   custom compressor. Note that if you have specified `:compression`, an
  #   appropriate compressor will be provided automatically.
  #
  # @option options [Cassandra::AddressResolution::Policy]
  #   :address_resolution_policy default:
  #   {Cassandra::AddressResolution::Policies::None No Resolution Policy} a custom address
  #   resolution policy. Note that if you have specified `:address_resolution`, an
  #   appropriate address resolution policy will be provided automatically.
  #
  # @option options [Object<#all, #error, #value, #promise>] :futures_factory
  #   default: {Cassandra::Future} a futures factory to assist with integration
  #   into existing futures library. Note that promises returned by this object
  #   must conform to {Cassandra::Promise} api, which is not yet public. Things
  #   may change, use at your own risk.
  #
  # @example Connecting to localhost
  #   cluster = Cassandra.cluster
  #
  # @example Configuring {Cassandra::Cluster}
  #   cluster = Cassandra.cluster(
  #               username: username,
  #               password: password,
  #               hosts: ['10.0.1.1', '10.0.1.2', '10.0.1.3']
  #             )
  #
  # @return [Cassandra::Cluster] a cluster instance
  def self.cluster(options = {})
    cluster_async(options).get
  end

  # Creates a {Cassandra::Cluster Cluster instance}.
  #
  # @see Cassandra.cluster
  #
  # @return [Cassandra::Future<Cassandra::Cluster>] a future resolving to the
  #   cluster instance.
  def self.cluster_async(options = {})
    options = validate_and_massage_options(options)
    hosts = []

    Array(options.fetch(:hosts, '127.0.0.1')).each do |host|
      case host
      when ::IPAddr
        hosts << host
      when ::String # ip address or hostname
        Resolv.each_address(host) do |ip|
          hosts << ::IPAddr.new(ip)
        end
      else
        raise ::ArgumentError, ":hosts must be String or IPAddr, #{host.inspect} given"
      end
    end

    if hosts.empty?
      raise ::ArgumentError,
            ":hosts #{options[:hosts].inspect} could not be resolved to any ip address"
    end

    hosts.shuffle!
  rescue => e
    futures = options.fetch(:futures_factory) { return Future::Error.new(e) }
    futures.error(e)
  else
    driver = Driver.new(options)
    driver.connect(hosts)
  end

  # @private
  SSL_CLASSES = [::TrueClass, ::FalseClass, ::OpenSSL::SSL::SSLContext].freeze

  # rubocop:disable Metrics/AbcSize
  # rubocop:disable Metrics/CyclomaticComplexity
  # rubocop:disable Metrics/PerceivedComplexity
  # @private
  def self.validate_and_massage_options(options)
    options = options.select do |key, _|
      CLUSTER_OPTIONS.include?(key)
    end

    has_username = options.key?(:username)
    has_password = options.key?(:password)
    if has_username || has_password
      if has_username && !has_password
        raise ::ArgumentError,
              'both :username and :password options must be specified, ' \
                  'but only :username given'
      end

      if !has_username && has_password
        raise ::ArgumentError,
              'both :username and :password options must be specified, ' \
                  'but only :password given'
      end

      username = options.delete(:username)
      password = options.delete(:password)

      Util.assert_instance_of(::String, username) do
        ":username must be a String, #{username.inspect} given"
      end
      Util.assert_instance_of(::String, password) do
        ":password must be a String, #{password.inspect} given"
      end
      Util.assert_not_empty(username) { ':username cannot be empty' }
      Util.assert_not_empty(password) { ':password cannot be empty' }

      options[:credentials]   = {username: username, password: password}
      options[:auth_provider] = Auth::Providers::Password.new(username, password)
    end

    if options.key?(:credentials)
      credentials = options[:credentials]

      Util.assert_instance_of(::Hash, credentials) do
        ":credentials must be a hash, #{credentials.inspect} given"
      end
    end

    if options.key?(:auth_provider)
      auth_provider = options[:auth_provider]

      Util.assert_responds_to(:create_authenticator, auth_provider) do
        ":auth_provider #{auth_provider.inspect} must respond to " \
            ":create_authenticator, but doesn't"
      end
    end

    has_client_cert = options.key?(:client_cert)
    has_private_key = options.key?(:private_key)

    if has_client_cert || has_private_key
      if has_client_cert && !has_private_key
        raise ::ArgumentError,
              'both :client_cert and :private_key options must be specified, ' \
                  'but only :client_cert given'
      end

      if !has_client_cert && has_private_key
        raise ::ArgumentError,
              'both :client_cert and :private_key options must be specified, ' \
                  'but only :private_key given'
      end

      Util.assert_instance_of(::String, options[:client_cert]) do
        ":client_cert must be a string, #{options[:client_cert].inspect} given"
      end
      Util.assert_instance_of(::String, options[:private_key]) do
        ":client_cert must be a string, #{options[:private_key].inspect} given"
      end
      client_cert = ::File.expand_path(options[:client_cert])
      private_key = ::File.expand_path(options[:private_key])

      Util.assert_file_exists(client_cert) do
        ":client_cert #{client_cert.inspect} doesn't exist"
      end
      Util.assert_file_exists(private_key) do
        ":private_key #{private_key.inspect} doesn't exist"
      end
    end

    has_server_cert = options.key?(:server_cert)

    if has_server_cert
      Util.assert_instance_of(::String, options[:server_cert]) do
        ":server_cert must be a string, #{options[:server_cert].inspect} given"
      end
      server_cert = ::File.expand_path(options[:server_cert])

      Util.assert_file_exists(server_cert) do
        ":server_cert #{server_cert.inspect} doesn't exist"
      end
    end

    if has_client_cert || has_server_cert
      context = ::OpenSSL::SSL::SSLContext.new

      if has_server_cert
        context.ca_file     = server_cert
        context.verify_mode = ::OpenSSL::SSL::VERIFY_PEER
      end

      if has_client_cert
        context.cert = ::OpenSSL::X509::Certificate.new(File.read(client_cert))

        context.key = if options.key?(:passphrase)
                        ::OpenSSL::PKey::RSA.new(File.read(private_key),
                                                 options[:passphrase])
                      else
                        ::OpenSSL::PKey::RSA.new(File.read(private_key))
                      end
      end

      options[:ssl] = context
    end

    if options.key?(:ssl)
      ssl = options[:ssl]

      Util.assert_instance_of_one_of(SSL_CLASSES, ssl) do
        ":ssl must be a boolean or an OpenSSL::SSL::SSLContext, #{ssl.inspect} given"
      end
    end

    if options.key?(:compression)
      compression = options.delete(:compression)

      case compression
      when :snappy
        options[:compressor] = Compression::Compressors::Snappy.new
      when :lz4
        options[:compressor] = Compression::Compressors::Lz4.new
      else
        raise ::ArgumentError,
              ":compression must be either :snappy or :lz4, #{compression.inspect} given"
      end
    end

    if options.key?(:compressor)
      compressor = options[:compressor]
      methods    = [:algorithm, :compress?, :compress, :decompress]

      Util.assert_responds_to_all(methods, compressor) do
        ":compressor #{compressor.inspect} must respond to #{methods.inspect}, " \
            "but doesn't"
      end
    end

    if options.key?(:logger)
      if options[:logger].nil?
        # Delete the key because we want to fallback to the default logger in Driver.
        options.delete(:logger)
      else
        # Validate
        logger = options[:logger]
        methods = [:debug, :info, :warn, :error, :fatal]

        Util.assert_responds_to_all(methods, logger) do
          ":logger #{logger.inspect} must respond to #{methods.inspect}, but doesn't"
        end
      end
    end

    if options.key?(:port)
      unless options[:port].nil?
        port = options[:port]
        Util.assert_instance_of(::Integer, port)
        Util.assert_one_of(1...2**16, port) do
          ":port must be a valid ip port, #{port} given"
        end
      end
    end

    options[:datacenter] = String(options[:datacenter]) if options.key?(:datacenter)

    if options.key?(:connect_timeout)
      timeout = options[:connect_timeout]

      unless timeout.nil?
        Util.assert_instance_of(::Numeric, timeout) do
          ":connect_timeout must be a number of seconds, #{timeout.inspect} given"
        end
        Util.assert(timeout > 0) do
          ":connect_timeout must be greater than 0, #{timeout} given"
        end
      end
    end

    if options.key?(:timeout)
      timeout = options[:timeout]

      unless timeout.nil?
        Util.assert_instance_of(::Numeric, timeout) do
          ":timeout must be a number of seconds, #{timeout.inspect} given"
        end
        Util.assert(timeout > 0) { ":timeout must be greater than 0, #{timeout} given" }
      end
    end

    if options.key?(:heartbeat_interval)
      timeout = options[:heartbeat_interval]

      unless timeout.nil?
        Util.assert_instance_of(::Numeric, timeout) do
          ":heartbeat_interval must be a number of seconds, #{timeout.inspect} given"
        end
        Util.assert(timeout > 0) do
          ":heartbeat_interval must be greater than 0, #{timeout} given"
        end
      end
    end

    if options.key?(:idle_timeout)
      timeout = options[:idle_timeout]

      unless timeout.nil?
        Util.assert_instance_of(::Numeric, timeout) do
          ":idle_timeout must be a number of seconds, #{timeout.inspect} given"
        end
        Util.assert(timeout > 0) do
          ":idle_timeout must be greater than 0, #{timeout} given"
        end
      end
    end

    if options.key?(:schema_refresh_delay)
      timeout = options[:schema_refresh_delay]

      Util.assert_instance_of(::Numeric, timeout) do
        ":schema_refresh_delay must be a number of seconds, #{timeout.inspect} given"
      end
      Util.assert(timeout > 0) do
        ":schema_refresh_delay must be greater than 0, #{timeout} given"
      end
    end

    if options.key?(:schema_refresh_timeout)
      timeout = options[:schema_refresh_timeout]

      Util.assert_instance_of(::Numeric, timeout) do
        ":schema_refresh_timeout must be a number of seconds, #{timeout.inspect} given"
      end
      Util.assert(timeout > 0) do
        ":schema_refresh_timeout must be greater than 0, #{timeout} given"
      end
    end

    if options.key?(:load_balancing_policy)
      load_balancing_policy = options[:load_balancing_policy]
      methods = [:host_up, :host_down, :host_found, :host_lost, :setup, :teardown,
                 :distance, :plan]

      Util.assert_responds_to_all(methods, load_balancing_policy) do
        ":load_balancing_policy #{load_balancing_policy.inspect} must respond " \
            "to #{methods.inspect}, but doesn't"
      end
    end

    if options.key?(:reconnection_policy)
      reconnection_policy = options[:reconnection_policy]

      Util.assert_responds_to(:schedule, reconnection_policy) do
        ":reconnection_policy #{reconnection_policy.inspect} must respond to " \
            ":schedule, but doesn't"
      end
    end

    if options.key?(:retry_policy)
      retry_policy = options[:retry_policy]
      methods = [:read_timeout, :write_timeout, :unavailable]

      Util.assert_responds_to_all(methods, retry_policy) do
        ":retry_policy #{retry_policy.inspect} must respond to #{methods.inspect}, " \
            "but doesn't"
      end
    end

    options[:listeners] = Array(options[:listeners]) if options.key?(:listeners)

    if options.key?(:consistency)
      consistency = options[:consistency]

      Util.assert_one_of(CONSISTENCIES, consistency) do
        ":consistency must be one of #{CONSISTENCIES.inspect}, " \
            "#{consistency.inspect} given"
      end
    end

    options[:nodelay] = !!options[:nodelay] if options.key?(:nodelay)
    options[:trace] = !!options[:trace] if options.key?(:trace)
    options[:shuffle_replicas] = !!options[:shuffle_replicas] if options.key?(:shuffle_replicas)

    if options.key?(:page_size)
      page_size = options[:page_size]

      unless page_size.nil?
        page_size = options[:page_size]
        Util.assert_instance_of(::Integer, page_size)
        Util.assert_one_of(1...2**32, page_size) do
          ":page_size must be a positive integer, #{page_size.inspect} given"
        end
      end
    end

    if options.key?(:protocol_version)
      protocol_version = options[:protocol_version]
      unless protocol_version.nil?
        Util.assert_instance_of(::Integer, protocol_version)
        Util.assert_one_of(1..4, protocol_version) do
          ":protocol_version must be a positive integer, #{protocol_version.inspect} given"
        end
      end
    end

    if options.key?(:futures_factory)
      futures_factory = options[:futures_factory]
      methods = [:error, :value, :promise, :all]

      Util.assert_responds_to_all(methods, futures_factory) do
        ":futures_factory #{futures_factory.inspect} must respond to " \
            "#{methods.inspect}, but doesn't"
      end
    end

    if options.key?(:address_resolution)
      address_resolution = options.delete(:address_resolution)

      case address_resolution
      when :none
        # do nothing
      when :ec2_multi_region
        options[:address_resolution_policy] =
          AddressResolution::Policies::EC2MultiRegion.new
      else
        raise ::ArgumentError,
              ':address_resolution must be either :none or :ec2_multi_region, ' \
                  "#{address_resolution.inspect} given"
      end
    end

    if options.key?(:address_resolution_policy)
      address_resolver = options[:address_resolution_policy]

      Util.assert_responds_to(:resolve, address_resolver) do
        ':address_resolution_policy must respond to :resolve, ' \
            "#{address_resolver.inspect} but doesn't"
      end
    end

    options[:synchronize_schema] = !!options[:synchronize_schema] if options.key?(:synchronize_schema)

    if options.key?(:client_timestamps)
      options[:timestamp_generator] = case options[:client_timestamps]
                                      when true
                                        if RUBY_ENGINE == 'jruby'
                                          Cassandra::TimestampGenerator::TickingOnDuplicate.new
                                        else
                                          Cassandra::TimestampGenerator::Simple.new
                                        end
				      when false
				        nil
                                      when :simple
                                        Cassandra::TimestampGenerator::Simple.new
                                      when :monotonic
                                        Cassandra::TimestampGenerator::TickingOnDuplicate.new
                                      else
				        # The value must be a generator instance.
                                        options[:client_timestamps]
                                    end

      if options[:timestamp_generator]
        Util.assert_responds_to(:next, options[:timestamp_generator]) do
          ":client_timestamps #{options[:client_timestamps].inspect} must be a boolean, :simple, :monotonic, or " \
          "an object that responds to :next"
        end
      end
      options.delete(:client_timestamps)
    end

    if options.key?(:connections_per_local_node)
      connections_per_node = options[:connections_per_local_node]

      unless connections_per_node.nil?
        connections_per_node = options[:connections_per_local_node]
        Util.assert_instance_of(::Integer, connections_per_node)
        Util.assert_one_of(1...2**16, connections_per_node) do
          ':connections_per_local_node must be a positive integer between ' \
              "1 and 65535, #{connections_per_node.inspect} given"
        end
      end
    end

    if options.key?(:connections_per_remote_node)
      connections_per_node = options[:connections_per_remote_node]

      unless connections_per_node.nil?
        connections_per_node = options[:connections_per_remote_node]
        Util.assert_instance_of(::Integer, connections_per_node)
        Util.assert_one_of(1...2**16, connections_per_node) do
          ':connections_per_remote_node must be a positive integer between ' \
              "1 and 65535, #{connections_per_node.inspect} given"
        end
      end
    end

    if options.key?(:requests_per_connection)
      requests_per_connection = options[:requests_per_connection]

      unless requests_per_connection.nil?
        requests_per_connection = options[:requests_per_connection]
        Util.assert_instance_of(::Integer, requests_per_connection)

        # v3 protocol says that max stream-id is 32767 (2^15-1). This setting might be
        # used to talk to a v2 (or less) node, but then we'll adjust it down.

        Util.assert_one_of(1...2**15, requests_per_connection) do
          ':requests_per_connection must be a positive integer, ' \
              "#{requests_per_connection.inspect} given"
        end
      end
    end
    options
  end

  # @private
  EMPTY_LIST = [].freeze
  # @private
  NOT_SET = ::Object.new
  # @private
  NULL_BYTE = "\x00".freeze

  # @private
  # ensures that:
  # ::Date.jd(DATE_OFFSET, ::Date::GREGORIAN)
  # => -5877641-06-23
  # ::Date.jd(DATE_OFFSET + 2 ** 31, ::Date::GREGORIAN)
  # => 1970-1-1
  # ::Date.jd(DATE_OFFSET + 2 ** 32, ::Date::GREGORIAN)
  # => 5881580-07-12
  DATE_OFFSET = (::Time.utc(1970, 1, 1).to_date.jd - 2**31)
end

require 'cassandra/attr_boolean'
require 'cassandra/version'
require 'cassandra/uuid'
require 'cassandra/time_uuid'
require 'cassandra/tuple'
require 'cassandra/udt'
require 'cassandra/time'

require 'cassandra/types'

require 'cassandra/errors'
require 'cassandra/compression'
require 'cassandra/protocol'
require 'cassandra/auth'
require 'cassandra/cassandra_logger'
require 'cassandra/null_logger'

require 'cassandra/executors'
require 'cassandra/future'
require 'cassandra/cluster'
require 'cassandra/driver'
require 'cassandra/host'
require 'cassandra/session'
require 'cassandra/result'
require 'cassandra/statement'
require 'cassandra/statements'

require 'cassandra/aggregate'
require 'cassandra/argument'
require 'cassandra/function'
require 'cassandra/function_collection'
require 'cassandra/column'
require 'cassandra/column_container'
require 'cassandra/table'
require 'cassandra/materialized_view'
require 'cassandra/keyspace'
require 'cassandra/index'

require 'cassandra/execution/info'
require 'cassandra/execution/options'
require 'cassandra/execution/trace'

require 'cassandra/load_balancing'
require 'cassandra/reconnection'
require 'cassandra/retry'
require 'cassandra/address_resolution'
require 'cassandra/timestamp_generator'

require 'cassandra/util'

# murmur3 hash extension
require 'cassandra_murmur3'

module Cassandra
  # @private
  VOID_STATEMENT = Statements::Void.new
  # @private
  VOID_OPTIONS   = Execution::Options.new(consistency: :one)
  # @private
  NO_HOSTS       = Errors::NoHostsAvailable.new
end
