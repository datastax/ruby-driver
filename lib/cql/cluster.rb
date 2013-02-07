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

  class Cluster
    attr_reader :keyspace

    def initialize(options={})
      connection_options = options.dup
      @connection_factory = connection_options.delete(:connection_factory) || Io::Connection
      @connection = @connection_factory.new(connection_options)
      @keyspace = options[:keyspace]
    end

    def start!
      return if @connection.connected?
      @connection.connect!
      execute_request!(Protocol::StartupRequest.new)
      use!(@keyspace) if @keyspace
      self
    end

    def shutdown!
      @connection.close!
      self
    end

    def use!(keyspace)
      raise NotConnectedError unless @connection.connected?
      execute!("USE #{keyspace}", :one) if check_keyspace_name!(keyspace)
    end

    def execute!(cql, consistency=:quorum)
      execute_request!(Protocol::QueryRequest.new(cql, consistency))
    end

    def execute_statement!(id, metadata, values, consistency)
      # TODO this does not take connection locality into consideration
      execute_request!(Protocol::ExecuteRequest.new(id, metadata, values, consistency))
    end

    def prepare!(cql)
      execute_request!(Protocol::PrepareRequest.new(cql))
    end

    private

    KEYSPACE_NAME_PATTERN = /^\w[\w\d_]*$/

    def check_keyspace_name!(name)
      if name !~ KEYSPACE_NAME_PATTERN
        raise InvalidKeyspaceNameError, %("#{name}" is not a valid keyspace name)
      end
      true
    end

    def execute_request!(request)
      raise NotConnectedError unless @connection.connected?
      interpret_response!(@connection.execute!(request))
    end

    def interpret_response!(response)
      case response
      when Protocol::ErrorResponse
        raise QueryError.new(response.code, response.message)
      when Protocol::RowsResultResponse
        QueryResult.new(response.metadata, response.rows)
      when Protocol::PreparedResultResponse
        PreparedStatement.new(self, response.id, response.metadata)
      when Protocol::SetKeyspaceResultResponse
        @keyspace = response.keyspace
        nil
      else
        nil
      end
    end

    class PreparedStatement
      def initialize(*args)
        @cluster, @id, @metadata = args
      end

      def execute!(*args)
        @cluster.execute_statement!(@id, @metadata, args, :quorum)
      end
    end

    class QueryResult
      include Enumerable

      attr_reader :metadata

      def initialize(metadata, rows)
        @metadata = ResultMetadata.new(metadata)
        @rows = rows
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