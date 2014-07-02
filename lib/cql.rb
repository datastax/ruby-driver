# encoding: utf-8

require 'ione'
require 'monitor'

module Cql
  CqlError = Class.new(StandardError)
  IoError = Ione::IoError

  # @private
  Promise = Ione::Promise

  # @private
  Future = Ione::Future

  Future.__send__(:alias_method, :get, :value)

  # @private
  Io = Ione::Io

  def self.cluster
    Builder.new
  end

  class Builder
    def initialize
      @options = {}
    end

    def with_contact_points(hosts)
      @options[:hosts] = hosts

      self
    end

    def build
      Cluster.new(@options.merge(:io_reactor => Io::IoReactor.new))
    end
  end

  class ThreadSafe < BasicObject
    kernel = ::Kernel.dup
    kernel.class_eval do
      [:to_s,:inspect,:=~,:!~,:===,:<=>,:eql?,:hash].each do |m|
        undef_method m
      end
    end

    include kernel, ::MonitorMixin

    def self.const_missing(n)
      ::Object.const_get(n)
    end

    def initialize(object)
      @object = object

      mon_initialize
    end

    def method_missing(m, *args, &block)
      synchronize do
        begin
          @object.respond_to?(m) ? @object.__send__(m, *args, &block) : super(m, *args, &block)
        ensure
          $@.delete_if {|t| %r"\A#{Regexp.quote(__FILE__)}:#{__LINE__-2}:"o =~ t} if $@
        end
      end
    end
  end

  class Cluster
    def initialize(options)
      @options  = ThreadSafe.new(options)
      @sessions = ThreadSafe.new([])
    end

    def connect_async(keyspace)
      options = @options.merge(:keyspace => keyspace).freeze
      client  = Client::AsynchronousClient.new(options)
      session = Session.new(client)

      client.connect.map { @sessions << session; session }
    end

    def connect(keyspace = nil)
      connect_async(keyspace).value
    end

    def close_async
      f = Future.all(*@sessions.map(&:close_async))
      f.on_complete {@sessions.clear}
      f
    end

    def close
      close_async.value

      self
    end
  end

  class Session
    def initialize(client)
      @client = client
    end

    def execute_async(cql, *args)
      case cql
      when Client::AsynchronousBatch
        cql.execute(args.shift)
      else
        @client.execute(cql, *args)
      end
    end

    def execute(cql, *args)
      execute_async(cql, *args).get
    end

    def prepare_async(cql)
      @client.prepare(cql)
    end

    def prepare(cql)
      prepare_async(cql).get
    end

    def batch
      batch = @client.batch(:logged)
      yield(batch) if block_given?
      batch
    end
    alias :logged_batch :batch

    def unlogged_batch
      batch = @client.batch(:unlogged)
      yield(batch) if block_given?
      batch
    end

    def counter_batch
      batch = @client.batch(:counter)
      yield(batch) if block_given?
      batch
    end

    def close_async
      @client.close
    end

    def close
      close_async.value
    end
  end
end

require 'cql/uuid'
require 'cql/time_uuid'
require 'cql/compression'
require 'cql/protocol'
require 'cql/auth'
require 'cql/client'
