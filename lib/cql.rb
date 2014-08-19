# encoding: utf-8

require 'ione'

require 'monitor'
require 'ipaddr'
require 'set'
require 'bigdecimal'
require 'forwardable'

module Cql
  # @private
  Io = Ione::Io

  # A list of all supported request consistencies
  # @see Cql::Session#execute_async
  CONSISTENCIES = [ :any, :one, :two, :three, :quorum, :all, :local_quorum,
                    :each_quorum, :serial, :local_serial, :local_one ].freeze

  # A list of all supported serial consistencies
  # @see Cql::Session#execute_async
  SERIAL_CONSISTENCIES = [:serial, :local_serial].freeze

  # Creates a {Cql::Cluster} instance
  #
  # @option options [Array<String, IPAddr>] :hosts (['127.0.0.1']) a list of
  #   initial addresses. Note that the entire cluster members will be
  #   discovered automatically once a connection to any hosts from the original
  #   list is successful.
  #
  # @option options [Hash{Symbol => String}] :credentials (none) hash with
  #   `:username` and `:password` keys
  #
  # @option options [Cql::Auth::Provider] :auth_provider (none) auth provider.
  #   Note that if you have specified `:credentials`, then a
  #   {Cql::Auth::Providers::PlainText} will be used automatically
  #
  # @option options [Symbol] :compression (none) compression to use. Must be
  #   either `:snappy` or `:lz4`. Also note, that in order for compression to
  #   work, you must install 'snappy' or 'lz4-ruby' gems.
  #
  # @option options [Logger] :logger (none) logger. a {Logger} instance from the
  #   standard library or any object responding to standard log methods
  #   (`#debug`, `#info`, `#warn`, `#error` and `#fatal`)
  #
  # @option options [Integer] :port (9042) cassandra native protocol port.
  #
  # @option options [Cql::LoadBalancing::Policy] :load_balancing_policy
  #   (Cql::LoadBalancing::Policies::RoundRobin) [a load balancing
  #   policy](/features/load_balancing).
  #
  # @option options [Cql::Reconnection::Policy] :reconnection_policy
  #   (Cql::Reconnection::Policies::Exponential) a reconnection policy to use.
  #   Note that default {Reconnection::Policies::Exponential} is configured
  #   with `Reconnection::Policies::Exponential.new(0.5, 30, 2)`
  #
  # @option options [Cql::Retry::Policy] :retry_policy
  #   (Retry::Policies::Default) a retry policy
  #
  # @option options [Array<Cql::Cluster::Listener>] :listeners (none) initial
  #   listeners. A list of initial cluster state listeners. Note that a
  #   load_balancing policy is automatically registered with the cluster.
  #
  # @example Connecting to localhost
  #   cluster = Cql.connect
  #
  # @example Configuring {Cql::Cluster}
  #   cluster = Cql.connect(
  #               credentials: {
  #                 :username => username,
  #                 :password => password
  #               },
  #               hosts: ['10.0.1.1', '10.0.1.2', '10.0.1.3']
  #             )
  #
  # @return [Cql::Cluster] a cluster instance
  def self.connect(options = {})
    options.select! do |key, value|
      [ :credentials, :auth_provider, :compression, :hosts, :logger, :port,
        :load_balancing_policy, :reconnection_policy, :retry_policy, :listeners
      ].include?(key)
    end

    if options.has_key?(:credentials)
      credentials = options[:credentials]

      unless credentials.has_key?(:username) && credentials.has_key?(:password)
        raise ::ArgumentError, ":credentials must be a hash with :username and :password, #{credentials.inspect} given"
      end

      options[:auth_provider] = Auth::Providers::PlainText.new(credentials[:username], credentials[:password])
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
        require 'cql/compression/snappy_compressor'
        options[:compressor] = Compression::SnappyCompressor.new
      when :lz4
        require 'cql/compression/lz4_compressor'
        options[:compressor] = Compression::Lz4Compressor.new
      else
        raise ::ArgumentError, ":compression must be either :snappy or :lz4, #{compression.inspect} given"
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

require 'cql/errors'
require 'cql/uuid'
require 'cql/time_uuid'
require 'cql/compression'
require 'cql/protocol'
require 'cql/auth'
require 'cql/client'

require 'cql/future'
require 'cql/cluster'
require 'cql/driver'
require 'cql/host'
require 'cql/reactor'
require 'cql/session'
require 'cql/result'
require 'cql/statement'
require 'cql/statements'

require 'cql/execution/info'
require 'cql/execution/options'
require 'cql/execution/trace'

require 'cql/load_balancing'
require 'cql/reconnection'
require 'cql/retry'

module Cql
  # @private
  VOID_STATEMENT = Statements::Void.new
  # @private
  VOID_OPTIONS   = Execution::Options.new({:consistency => :one})
  # @private
  NO_HOSTS       = Errors::NoHostsAvailable.new
end
