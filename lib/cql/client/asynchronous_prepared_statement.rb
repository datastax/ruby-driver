# encoding: utf-8

module Cql
  module Client
    # @private
    class AsynchronousPreparedStatement < PreparedStatement
      # @private
      def initialize(cql, execute_options_decoder, connection_manager, logger)
        @cql = cql
        @execute_options_decoder = execute_options_decoder
        @connection_manager = connection_manager
        @logger = logger
        @request_runner = RequestRunner.new
      end

      def self.prepare(cql, execute_options_decoder, connection_manager, logger)
        statement = new(cql, execute_options_decoder, connection_manager, logger)
        futures = connection_manager.map do |connection|
          statement.prepare(connection)
        end
        Future.all(*futures).map { statement }
      rescue => e
        Future.failed(e)
      end

      def execute(*args)
        connection = @connection_manager.random_connection
        if connection[self]
          run(args, connection)
        else
          prepare(connection).flat_map do
            run(args, connection)
          end
        end
      rescue => e
        Future.failed(e)
      end

      # @private
      def prepare(connection)
        prepare_request = Protocol::PrepareRequest.new(@cql)
        f = @request_runner.execute(connection, prepare_request) do |response|
          connection[self] = response.id
          unless @raw_metadata
            # NOTE: this is not thread safe, but the worst that could happen
            # is that we assign the same data multiple times
            @raw_metadata = response.metadata
            @metadata = ResultMetadata.new(@raw_metadata)
          end
          @logger.debug('Statement prepared on new connection')
        end
        f.map { self }
      end

      private

      def run(args, connection)
        statement_id = connection[self]
        bound_args = args.shift(@raw_metadata.size)
        unless bound_args.size == @raw_metadata.size && args.size <= 1
          raise ArgumentError, "Expected #{@raw_metadata.size} arguments, got #{bound_args.size}"
        end
        consistency, timeout, trace = @execute_options_decoder.decode_options(args.last)
        statement_id = connection[self]
        request = Protocol::ExecuteRequest.new(statement_id, @raw_metadata, bound_args, consistency, trace)
        @request_runner.execute(connection, request, timeout)
      end
    end
  end
end