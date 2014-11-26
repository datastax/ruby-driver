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


require 'ione'
require 'json'

require 'monitor'
require 'ipaddr'
require 'set'
require 'bigdecimal'
require 'forwardable'
require 'timeout'
require 'digest'
require 'stringio'
require 'resolv'
require 'openssl'
require 'securerandom'
require 'time'

module Cassandra
  # A list of all supported request consistencies
  # @see http://www.datastax.com/documentation/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html Consistency levels in Apache Cassandra 2.0
  # @see http://www.datastax.com/documentation/cassandra/1.2/cassandra/dml/dml_config_consistency_c.html Consistency levels in Apache Cassandra 1.2
  # @see Cassandra::Session#execute_async
  CONSISTENCIES = [ :any, :one, :two, :three, :quorum, :all, :local_quorum,
                    :each_quorum, :serial, :local_serial, :local_one ].freeze

  # A list of all supported serial consistencies
  # @see Cassandra::Session#execute_async
  SERIAL_CONSISTENCIES = [:serial, :local_serial].freeze

  # A list of all possible write types that a
  # {Cassandra::Errors::WriteTimeoutError} can have.
  #
  # @see https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec#L591-L603 Description of possible types of writes in Apache Cassandra native protocol spec v1
  WRITE_TYPES = [:simple, :batch, :unlogged_batch, :counter, :batch_log].freeze

  # Creates a {Cassandra::Cluster} instance
  #
  # @option options [Array<String, IPAddr>] :hosts (['127.0.0.1']) a list of
  #   initial addresses. Note that the entire list of cluster members will be
  #   discovered automatically once a connection to any hosts from the original
  #   list is successful.
  #
  # @option options [Integer] :port (9042) cassandra native protocol port.
  #
  # @option options [String] :datacenter (nil) name of current datacenter.
  #   First datacenter found will be assumed current by default. Note that you
  #   can skip this option if you specify only hosts from the local datacenter
  #   in `:hosts` option.
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
  # @option options [Boolean] :synchronize_schema (true) whether the driver
  #   should automatically keep schema metadata synchronized. When enabled, the
  #   driver updates schema metadata after receiving schema change
  #   notifications from Cassandra. Setting this setting to `false` disables
  #   automatic schema updates. Schema metadata is used by the driver to
  #   determine cluster partitioners as well as to find partition keys and
  #   replicas of prepared statements, this information makes token aware load
  #   balancing possible. One can still use {Cassandra::Cluster#refresh_schema}
  #   to refresh schema manually.
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
  #   default: {Cassandra::Reconnection::Policies::Exponential}. Note that the
  #   default policy is configured with `(0.5, 30, 2)`.
  #
  # @option options [Cassandra::Retry::Policy] :retry_policy default:
  #   {Cassandra::Retry::Policies::Default}.
  #
  # @option options [Logger] :logger (none) logger. a {Logger} instance from the
  #   standard library or any object responding to standard log methods
  #   (`#debug`, `#info`, `#warn`, `#error` and `#fatal`).
  #
  # @option options [Enumerable<Cassandra::Listener>] :listeners (none)
  #   initial listeners. A list of initial cluster state listeners. Note that a
  #   `:load_balancing` policy is automatically registered with the cluster.
  #
  # @option options [Symbol] :consistency (:one) default consistency to use for
  #   all requests. Must be one of {Cassandra::CONSISTENCIES}.
  #
  # @option options [Boolean] :trace (false) whether or not to trace all
  #   requests by default.
  #
  # @option options [Integer] :page_size (10000) default page size for all
  #   select queries. Set this value to `nil` to disable paging.
  #
  # @option options [Hash{String => String}] :credentials (none) a hash of credentials - to be used with [credentials authentication in cassandra 1.2](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec#L238-L250). Note that if you specified `:username` and `:password` options, those credentials are configured automatically.
  #
  # @option options [Cassandra::Auth::Provider] :auth_provider (none) a custom auth provider to be used with [SASL authentication in cassandra 2.0](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v2.spec#L257-L273). Note that if you have specified `:username` and `:password`, then a {Cassandra::Auth::Providers::Password} will be used automatically.
  #
  # @option options [Cassandra::Compression::Compressor] :compressor (none) a
  #   custom compressor. Note that if you have specified `:compression`, an
  #   appropriate compressor will be provided automatically.
  #
  # @option options [Cassandra::AddressResolution::Policy]
  #   :address_resolution_policy default:
  #   {Cassandra::AddressResolution::Policies::None} a custom address resolution
  #   policy. Note that if you have specified `:address_resolution`, an
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

  # Creates a {Cassandra::Cluster} instance
  #
  # @see Cassandra.cluster
  #
  # @return [Cassandra::Future<Cassandra::Cluster>] a future resolving to the
  #   cluster instance.
  def self.cluster_async(options = {})
    options = options.select do |key, value|
      [ :credentials, :auth_provider, :compression, :hosts, :logger, :port,
        :load_balancing_policy, :reconnection_policy, :retry_policy, :listeners,
        :consistency, :trace, :page_size, :compressor, :username, :password,
        :ssl, :server_cert, :client_cert, :private_key, :passphrase,
        :connect_timeout, :futures_factory, :datacenter, :address_resolution,
        :address_resolution_policy, :idle_timeout, :heartbeat_interval, :timeout,
        :synchronize_schema, :schema_refresh_delay, :schema_refresh_timeout
      ].include?(key)
    end

    futures = options.fetch(:futures_factory, Future)

    has_username = options.has_key?(:username)
    has_password = options.has_key?(:password)
    if has_username || has_password
      if has_username && !has_password
        raise ::ArgumentError, "both :username and :password options must be specified, but only :username given"
      end

      if !has_username && has_password
        raise ::ArgumentError, "both :username and :password options must be specified, but only :password given"
      end

      username = options.delete(:username)
      password = options.delete(:password)

      Util.assert_instance_of(::String, username) { ":username must be a String, #{username.inspect} given" }
      Util.assert_instance_of(::String, password) { ":password must be a String, #{password.inspect} given" }
      Util.assert_not_empty(username) { ":username cannot be empty" }
      Util.assert_not_empty(password) { ":password cannot be empty" }

      options[:credentials]   = {:username => username, :password => password}
      options[:auth_provider] = Auth::Providers::Password.new(username, password)
    end

    if options.has_key?(:credentials)
      credentials = options[:credentials]

      Util.assert_instance_of(::Hash, credentials) { ":credentials must be a hash, #{credentials.inspect} given" }
    end

    if options.has_key?(:auth_provider)
      auth_provider = options[:auth_provider]

      Util.assert_responds_to(:create_authenticator, auth_provider) { ":auth_provider #{auth_provider.inspect} must respond to :create_authenticator, but doesn't" }
    end

    has_client_cert = options.has_key?(:client_cert)
    has_private_key = options.has_key?(:private_key)

    if has_client_cert || has_private_key
      if has_client_cert && !has_private_key
        raise ::ArgumentError, "both :client_cert and :private_key options must be specified, but only :client_cert given"
      end

      if !has_client_cert && has_private_key
        raise ::ArgumentError, "both :client_cert and :private_key options must be specified, but only :private_key given"
      end

      client_cert = ::File.expand_path(options[:client_cert])
      private_key = ::File.expand_path(options[:private_key])

      Util.assert_file_exists(client_cert) { ":client_cert #{client_cert.inspect} doesn't exist" }
      Util.assert_file_exists(private_key) { ":private_key #{private_key.inspect} doesn't exist" }
    end

    has_server_cert = options.has_key?(:server_cert)

    if has_server_cert
      server_cert = ::File.expand_path(options[:server_cert])

      Util.assert_file_exists(server_cert) { ":server_cert #{server_cert.inspect} doesn't exist" }
    end

    if has_client_cert || has_server_cert
      context = ::OpenSSL::SSL::SSLContext.new

      if has_server_cert
        context.ca_file     = server_cert
        context.verify_mode = ::OpenSSL::SSL::VERIFY_PEER
      end

      if has_client_cert
        context.cert = ::OpenSSL::X509::Certificate.new(File.read(client_cert))

        if options.has_key?(:passphrase)
          context.key = ::OpenSSL::PKey::RSA.new(File.read(private_key), options[:passphrase])
        else
          context.key = ::OpenSSL::PKey::RSA.new(File.read(private_key))
        end
      end

      options[:ssl] = context
    end

    if options.has_key?(:ssl)
      ssl = options[:ssl]

      Util.assert_instance_of_one_of([::TrueClass, ::FalseClass, ::OpenSSL::SSL::SSLContext], ssl) { ":ssl must be a boolean or an OpenSSL::SSL::SSLContext, #{ssl.inspect} given" }
    end

    if options.has_key?(:compression)
      compression = options.delete(:compression)

      case compression
      when :snappy
        require 'cassandra/compression/compressors/snappy'
        options[:compressor] = Compression::Compressors::Snappy.new
      when :lz4
        require 'cassandra/compression/compressors/lz4'
        options[:compressor] = Compression::Compressors::Lz4.new
      else
        raise ::ArgumentError, ":compression must be either :snappy or :lz4, #{compression.inspect} given"
      end
    end

    if options.has_key?(:compressor)
      compressor = options[:compressor]
      methods    = [:algorithm, :compress?, :compress, :decompress]

      Util.assert_responds_to_all(methods, compressor) { ":compressor #{compressor.inspect} must respond to #{methods.inspect}, but doesn't" }
    end

    if options.has_key?(:logger)
      logger  = options[:logger]
      methods = [:debug, :info, :warn, :error, :fatal]

      Util.assert_responds_to_all(methods, logger) { ":logger #{logger.inspect} must respond to #{methods.inspect}, but doesn't" }
    end

    if options.has_key?(:port)
      port = options[:port] = Integer(options[:port])

      Util.assert_one_of(0..65536, port) { ":port must be a valid ip port, #{port} given" }
    end

    if options.has_key?(:datacenter)
      options[:datacenter] = String(options[:datacenter])
    end

    if options.has_key?(:connect_timeout)
      timeout = options[:connect_timeout]

      unless timeout.nil?
        Util.assert_instance_of(::Numeric, timeout) { ":connect_timeout must be a number of seconds, #{timeout} given" }
        Util.assert(timeout > 0) { ":connect_timeout must be greater than 0, #{timeout} given" }
      end
    end

    if options.has_key?(:timeout)
      timeout = options[:timeout]

      unless timeout.nil?
        Util.assert_instance_of(::Numeric, timeout) { ":timeout must be a number of seconds, #{timeout} given" }
        Util.assert(timeout > 0) { ":timeout must be greater than 0, #{timeout} given" }
      end
    end

    if options.has_key?(:heartbeat_interval)
      timeout = options[:heartbeat_interval]

      unless timeout.nil?
        Util.assert_instance_of(::Numeric, timeout) { ":heartbeat_interval must be a number of seconds, #{timeout} given" }
        Util.assert(timeout > 0) { ":heartbeat_interval must be greater than 0, #{timeout} given" }
      end
    end

    if options.has_key?(:idle_timeout)
      timeout = options[:idle_timeout]

      unless timeout.nil?
        Util.assert_instance_of(::Numeric, timeout) { ":idle_timeout must be a number of seconds, #{timeout} given" }
        Util.assert(timeout > 0) { ":idle_timeout must be greater than 0, #{timeout} given" }
      end
    end

    if options.has_key?(:schema_refresh_delay)
      timeout = options[:schema_refresh_delay]

      Util.assert_instance_of(::Numeric, timeout) { ":schema_refresh_delay must be a number of seconds, #{timeout} given" }
      Util.assert(timeout > 0) { ":schema_refresh_delay must be greater than 0, #{timeout} given" }
    end

    if options.has_key?(:schema_refresh_timeout)
      timeout = options[:schema_refresh_timeout]

      Util.assert_instance_of(::Numeric, timeout) { ":schema_refresh_timeout must be a number of seconds, #{timeout} given" }
      Util.assert(timeout > 0) { ":schema_refresh_timeout must be greater than 0, #{timeout} given" }
    end

    if options.has_key?(:load_balancing_policy)
      load_balancing_policy = options[:load_balancing_policy]
      methods = [:host_up, :host_down, :host_found, :host_lost, :setup, :teardown, :distance, :plan]

      Util.assert_responds_to_all(methods, load_balancing_policy) { ":load_balancing_policy #{load_balancing_policy.inspect} must respond to #{methods.inspect}, but doesn't" }
    end

    if options.has_key?(:reconnection_policy)
      reconnection_policy = options[:reconnection_policy]

      Util.assert_responds_to(:schedule, reconnection_policy) { ":reconnection_policy #{reconnection_policy.inspect} must respond to :schedule, but doesn't" }
    end

    if options.has_key?(:retry_policy)
      retry_policy = options[:retry_policy]
      methods = [:read_timeout, :write_timeout, :unavailable]

      Util.assert_responds_to_all(methods, retry_policy) { ":retry_policy #{retry_policy.inspect} must respond to #{methods.inspect}, but doesn't" }
    end

    if options.has_key?(:listeners)
      options[:listeners] = Array(options[:listeners])
    end

    if options.has_key?(:consistency)
      consistency = options[:consistency]

      Util.assert_one_of(CONSISTENCIES, consistency) { ":consistency must be one of #{CONSISTENCIES.inspect}, #{consistency.inspect} given" }
    end

    if options.has_key?(:trace)
      options[:trace] = !!options[:trace]
    end

    if options.has_key?(:page_size)
      page_size = options[:page_size]

      unless page_size.nil?
        page_size = options[:page_size] = Integer(page_size)
        Util.assert(page_size > 0) { ":page_size must be a positive integer, #{page_size.inspect} given" }
      end
    end

    if options.has_key?(:futures_factory)
      futures_factory = options[:futures_factory]
      methods = [:error, :value, :promise, :all]

      Util.assert_responds_to_all(methods, futures_factory) { ":futures_factory #{futures_factory.inspect} must respond to #{methods.inspect}, but doesn't" }
    end

    if options.has_key?(:address_resolution)
      address_resolution = options.delete(:address_resolution)

      case address_resolution
      when :none
        # do nothing
      when :ec2_multi_region
        options[:address_resolution_policy] = AddressResolution::Policies::EC2MultiRegion.new
      else
        raise ::ArgumentError, ":address_resolution must be either :none or :ec2_multi_region, #{address_resolution.inspect} given"
      end
    end

    if options.has_key?(:address_resolution_policy)
      address_resolver = options[:address_resolution_policy]

      Util.assert_responds_to(:resolve, address_resolver) { ":address_resolution_policy must respond to :resolve, #{address_resolver.inspect} but doesn't" }
    end

    if options.has_key?(:synchronize_schema)
      options[:synchronize_schema] = !!options[:synchronize_schema]
    end

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
      raise ::ArgumentError, ":hosts #{options[:hosts].inspect} could not be resolved to any ip address"
    end
  rescue => e
    futures.error(e)
  else
    promise = futures.promise

    Driver.new(options).connect(hosts).on_complete do |f|
      if f.resolved?
        promise.fulfill(f.value)
      else
        f.on_failure {|e| promise.break(e)}
      end
    end

    promise.future
  end
end

require 'cassandra/errors'
require 'cassandra/uuid'
require 'cassandra/time_uuid'
require 'cassandra/compression'
require 'cassandra/protocol'
require 'cassandra/auth'
require 'cassandra/null_logger'

require 'cassandra/future'
require 'cassandra/cluster'
require 'cassandra/driver'
require 'cassandra/host'
require 'cassandra/session'
require 'cassandra/result'
require 'cassandra/statement'
require 'cassandra/statements'

require 'cassandra/column'
require 'cassandra/table'
require 'cassandra/keyspace'

require 'cassandra/execution/info'
require 'cassandra/execution/options'
require 'cassandra/execution/trace'

require 'cassandra/load_balancing'
require 'cassandra/reconnection'
require 'cassandra/retry'
require 'cassandra/address_resolution'

require 'cassandra/util'

# murmur3 hash extension
require 'cassandra_murmur3'

module Cassandra
  # @private
  VOID_STATEMENT = Statements::Void.new
  # @private
  VOID_OPTIONS   = Execution::Options.new({:consistency => :one})
  # @private
  NO_HOSTS       = Errors::NoHostsAvailable.new
  # @private
  EMPTY_LIST = [].freeze
end
