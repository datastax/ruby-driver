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

require 'ione'

require 'monitor'
require 'ipaddr'
require 'set'
require 'bigdecimal'
require 'forwardable'

module Cassandra
  # A list of all supported request consistencies
  # @see Cassandra::Session#execute_async
  CONSISTENCIES = [ :any, :one, :two, :three, :quorum, :all, :local_quorum,
                    :each_quorum, :serial, :local_serial, :local_one ].freeze

  # A list of all supported serial consistencies
  # @see Cassandra::Session#execute_async
  SERIAL_CONSISTENCIES = [:serial, :local_serial].freeze

  # Creates a {Cassandra::Cluster} instance
  #
  # @option options [Array<String, IPAddr>] :hosts (['127.0.0.1']) a list of
  #   initial addresses. Note that the entire list of cluster members will be
  #   discovered automatically once a connection to any hosts from the original
  #   list is successful
  #
  # @option options [Integer] :port (9042) cassandra native protocol port
  #
  # @option options [String] :username (none) username to use for
  #   authentication to cassandra. Note that you must also specify `:password`
  #
  # @option options [String] :password (none) password to use for
  #   authentication to cassandra. Note that you must also specify `:username`
  #
  # @option options [Symbol] :compression (none) compression to use. Must be
  #   either `:snappy` or `:lz4`. Also note, that in order for compression to
  #   work, you must install 'snappy' or 'lz4-ruby' gems
  #
  # @option options [Cassandra::LoadBalancing::Policy] :load_balancing_policy
  #   default: {Cassandra::LoadBalancing::Policies::RoundRobin}
  #
  # @option options [Cassandra::Reconnection::Policy] :reconnection_policy
  #   default: {Cassandra::Reconnection::Policies::Exponential}. Note that the
  #   default policy is configured with
  #   `Reconnection::Policies::Exponential.new(0.5, 30, 2)`
  #
  # @option options [Cassandra::Retry::Policy] :retry_policy default:
  #   {Cassandra::Retry::Policies::Default}
  #
  # @option options [Logger] :logger (none) logger. a {Logger} instance from the
  #   standard library or any object responding to standard log methods
  #   (`#debug`, `#info`, `#warn`, `#error` and `#fatal`)
  #
  # @option options [Enumerable<Cassandra::Listener>] :listeners (none)
  #   initial listeners. A list of initial cluster state listeners. Note that a
  #   load_balancing policy is automatically registered with the cluster.
  #
  # @option options [Symbol] :consistency (:quorum) default consistency to use
  #   for all requests. Must be one of {Cassandra::CONSISTENCIES}
  #
  # @option options [Boolean] :trace (false) whether or not to trace all
  #   requests by default
  #
  # @option options [Integer] :page_size (nil) default page size for all select
  #   queries
  #
  # @option options [Hash{String => String}] :credentials (none) a hash of credentials - to be used with [credentials authentication in cassandra 1.2](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v1.spec#L238-L250). Note that if you specified `:username` and `:password` options, those credentials are configured automatically
  #
  # @option options [Cassandra::Auth::Provider] :auth_provider (none) a custom auth provider to be used with [SASL authentication in cassandra 2.0](https://github.com/apache/cassandra/blob/trunk/doc/native_protocol_v2.spec#L257-L273). Note that if you have specified `:username` and `:password`, then a {Cassandra::Auth::Providers::Password} will be used automatically
  #
  # @option options [Cassandra::Compressor] :compressor (none) a custom
  #   compressor. Note that if you have specified `:compression`, an
  #   appropriate compressor will be provided automatically
  #
  # @example Connecting to localhost
  #   cluster = Cassandra.connect
  #
  # @example Configuring {Cassandra::Cluster}
  #   cluster = Cassandra.connect(
  #               username: username,
  #               password: password,
  #               hosts: ['10.0.1.1', '10.0.1.2', '10.0.1.3']
  #             )
  #
  # @return [Cassandra::Cluster] a cluster instance
  def self.connect(options = {})
    options.select! do |key, value|
      [ :credentials, :auth_provider, :compression, :hosts, :logger, :port,
        :load_balancing_policy, :reconnection_policy, :retry_policy, :listeners,
        :consistency, :trace, :page_size, :compressor, :username, :password
      ].include?(key)
    end

    has_username = options.has_key?(:username)
    has_password = options.has_key?(:password)
    if has_username || has_password
      if has_username && !has_password
        raise ::ArgumentError, "both :username and :password options must be specified, but only :username given"
      end

      if !has_username && has_password
        raise ::ArgumentError, "both :username and :password options must be specified, but only :password given"
      end

      username = String(options.delete(:username))
      password = String(options.delete(:password))

      raise ::ArgumentError, ":username cannot be empty" if username.empty?
      raise ::ArgumentError, ":password cannot be empty" if password.empty?

      options[:credentials]   = {:username => username, :password => password}
      options[:auth_provider] = Auth::Providers::PlainText.new(username, password)
    end

    if options.has_key?(:credentials)
      credentials = options[:credentials]

      unless credentials.is_a?(Hash)
        raise ::ArgumentError, ":credentials must be a hash, #{credentials.inspect} given"
      end
    end

    if options.has_key?(:auth_provider)
      auth_provider = options[:auth_provider]

      unless auth_provider.respond_to?(:create_authenticator)
        raise ::ArgumentError, ":auth_provider #{auth_provider.inspect} must respond to :create_authenticator, but doesn't"
      end
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

      unless methods.all? {|method| compressor.respond_to?(method)}
        raise ::ArgumentError, ":compressor #{compressor.inspect} must respond to #{methods.inspect}, but doesn't"
      end
    end

    if options.has_key?(:logger)
      logger  = options[:logger]
      methods = [:debug, :info, :warn, :error, :fatal]

      unless methods.all? {|method| logger.respond_to?(method)}
        raise ::ArgumentError, ":logger #{logger.inspect} must respond to #{methods.inspect}, but doesn't"
      end
    end

    if options.has_key?(:port)
      port = options[:port] = Integer(options[:port])

      if port < 0 || port > 65536
        raise ::ArgumentError, ":port must be a valid ip port, #{port.given}"
      end
    end

    if options.has_key?(:load_balancing_policy)
      load_balancing_policy = options[:load_balancing_policy]
      methods = [:host_up, :host_down, :host_found, :host_lost, :distance, :plan]

      unless methods.all? {|method| load_balancing_policy.respond_to?(method)}
        raise ::ArgumentError, ":load_balancing_policy #{load_balancing_policy.inspect} must respond to #{methods.inspect}, but doesn't"
      end
    end

    if options.has_key?(:reconnection_policy)
      reconnection_policy = options[:reconnection_policy]

      unless reconnection_policy.respond_to?(:schedule)
        raise ::ArgumentError, ":reconnection_policy #{reconnection_policy.inspect} must respond to :schedule, but doesn't"
      end
    end

    if options.has_key?(:retry_policy)
      retry_policy = options[:retry_policy]
      methods = [:read_timeout, :write_timeout, :unavailable]

      unless methods.all? {|method| retry_policy.respond_to?(method)}
        raise ::ArgumentError, ":retry_policy #{retry_policy.inspect} must respond to #{methods.inspect}, but doesn't"
      end
    end

    if options.has_key?(:listeners)
      listeners = options[:listeners]

      unless listeners.respond_to?(:each)
        raise ::ArgumentError, ":listeners must be an Enumerable, #{listeners.inspect} given"
      end
    end

    if options.has_key?(:consistency)
      consistency = options[:consistency]

      unless CONSISTENCIES.include?(consistency)
        raise ::ArgumentError, ":consistency must be one of #{CONSISTENCIES.inspect}, #{consistency.inspect} given"
      end
    end

    if options.has_key?(:trace)
      options[:trace] = !!options[:trace]
    end

    if options.has_key?(:page_size)
      page_size = options[:page_size] = Integer(options[:page_size])

      if page_size <= 0
        raise ::ArgumentError, ":page_size must be a positive integer, #{page_size.inspect} given"
      end
    end

    hosts = options.fetch(:hosts, [])
    hosts << ::IPAddr.new('127.0.0.1') if hosts.empty?

    hosts.map! do |host|
      case host
      when ::IPAddr
        host
      when ::String
        ::IPAddr.new(host)
      else
        raise ::ArgumentError, ":hosts must be String or IPAddr, #{host.inspect} given"
      end
    end

    Driver.new(options).connect(hosts).value
  end
end

require 'cassandra/errors'
require 'cassandra/uuid'
require 'cassandra/time_uuid'
require 'cassandra/compression'
require 'cassandra/protocol'
require 'cassandra/auth'
require 'cassandra/client'

require 'cassandra/future'
require 'cassandra/cluster'
require 'cassandra/driver'
require 'cassandra/host'
require 'cassandra/session'
require 'cassandra/result'
require 'cassandra/statement'
require 'cassandra/statements'

require 'cassandra/execution/info'
require 'cassandra/execution/options'
require 'cassandra/execution/trace'

require 'cassandra/load_balancing'
require 'cassandra/reconnection'
require 'cassandra/retry'

module Cassandra
  # @private
  Io = Ione::Io
  # @private
  VOID_STATEMENT = Statements::Void.new
  # @private
  VOID_OPTIONS   = Execution::Options.new({:consistency => :one})
  # @private
  NO_HOSTS       = Errors::NoHostsAvailable.new
end
