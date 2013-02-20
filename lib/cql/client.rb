# encoding: utf-8

module Cql
  NotConnectedError = Class.new(CqlError)
  InvalidKeyspaceNameError = Class.new(CqlError)

  class QueryError < CqlError
    attr_reader :code

    def initialize(code, message)
      super(message)
      @code = code
    end
  end

  class Client
    def initialize(options={})
      connection_timeout = options[:connection_timeout]
      @host = options[:host] || 'localhost'
      @port = options[:port] || 9042
      @io_reactor = options[:io_reactor] || Io::IoReactor.new(connection_timeout: connection_timeout)
      @lock = Mutex.new
      @started = false
      @shut_down = false
      @initial_keyspace = options[:keyspace]
      @connection_keyspaces = {}
    end

    def start!
      @lock.synchronize do
        return if @started
        @started = true
      end
      @io_reactor.start
      hosts = @host.split(',')
      start_request = Protocol::StartupRequest.new
      connection_futures = hosts.map do |host|
        @io_reactor.add_connection(host, @port).flat_map do |connection_id|
          execute_request(start_request, connection_id).map { connection_id }
        end
      end
      @connection_ids = Future.combine(*connection_futures).get
      use(@initial_keyspace) if @initial_keyspace
      self
    end

    def shutdown!
      @lock.synchronize do
        return if @shut_down
        @shut_down = true
        @started = false
      end
      @io_reactor.stop.get
      self
    end

    def keyspace
      @lock.synchronize do
        return @connection_ids.map { |id| @connection_keyspaces[id] }.first
      end
    end

    def use(keyspace, connection_ids=@connection_ids)
      raise NotConnectedError unless @started
      if check_keyspace_name!(keyspace)
        @lock.synchronize do
          connection_ids = connection_ids.select { |id| @connection_keyspaces[id] != keyspace }
        end
        if connection_ids.any?
          futures = connection_ids.map do |connection_id|
            execute_request(Protocol::QueryRequest.new("USE #{keyspace}", :one), connection_id)
          end
          futures.compact!
          Future.combine(*futures).get
        end
        nil
      end
    end

    def execute(cql, consistency=:quorum)
      result = execute_request(Protocol::QueryRequest.new(cql, consistency)).value
      ensure_keyspace!
      result
    end

    def execute_statement(connection_id, statement_id, metadata, values, consistency)
      execute_request(Protocol::ExecuteRequest.new(statement_id, metadata, values, consistency), connection_id).value
    end

    def prepare(cql)
      execute_request(Protocol::PrepareRequest.new(cql)).value
    end

    private

    KEYSPACE_NAME_PATTERN = /^\w[\w\d_]*$/

    def check_keyspace_name!(name)
      if name !~ KEYSPACE_NAME_PATTERN
        raise InvalidKeyspaceNameError, %("#{name}" is not a valid keyspace name)
      end
      true
    end

    def execute_request(request, connection_id=nil)
      raise NotConnectedError unless @started
      @io_reactor.queue_request(request, connection_id).map do |response, connection_id|
        interpret_response!(response, connection_id)
      end
    end

    def interpret_response!(response, connection_id)
      case response
      when Protocol::ErrorResponse
        raise QueryError.new(response.code, response.message)
      when Protocol::RowsResultResponse
        QueryResult.new(response.metadata, response.rows)
      when Protocol::PreparedResultResponse
        PreparedStatement.new(self, connection_id, response.id, response.metadata)
      when Protocol::SetKeyspaceResultResponse
        @lock.synchronize do
          @last_keyspace_change = @connection_keyspaces[connection_id] = response.keyspace
        end
        nil
      else
        nil
      end
    end

    def ensure_keyspace!
      ks = nil
      @lock.synchronize do
        ks = @last_keyspace_change
        return unless @last_keyspace_change
      end
      use(ks, @connection_ids) if ks
    end

    class PreparedStatement
      def initialize(*args)
        @client, @connection_id, @statement_id, @metadata = args
      end

      def execute(*args)
        @client.execute_statement(@connection_id, @statement_id, @metadata, args, :quorum)
      end
    end

    class QueryResult
      include Enumerable

      attr_reader :metadata

      def initialize(metadata, rows)
        @metadata = ResultMetadata.new(metadata)
        @rows = rows
      end

      def empty?
        @rows.empty?
      end

      def each(&block)
        @rows.each(&block)
      end
    end

    class ResultMetadata
      include Enumerable

      def initialize(metadata)
        @metadata = Hash[metadata.map { |m| mm = ColumnMetadata.new(*m); [mm.column_name, mm] }]
      end

      def [](column_name)
        @metadata[column_name]
      end

      def each(&block)
        @metadata.each_value(&block)
      end
    end

    class ColumnMetadata
      attr_reader :keyspace, :table, :table, :column_name, :type
      
      def initialize(*args)
        @keyspace, @table, @column_name, @type = args
      end

      def to_ary
        [@keyspace, @table, @column_name, @type]
      end
    end
  end
end