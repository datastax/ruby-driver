# encoding: utf-8

module Cql
  module Client
    # Batches let you send multiple queries (`INSERT`, `UPDATE` and `DELETE`) in
    # one go. This can lead to better performance, and depending on the options
    # you specify can also give you different consistency guarantees.
    #
    # Batches can contain a mix of different queries and prepared statements.
    #
    # @see Cql::Client::Client#batch
    class Batch
      # @!method add(cql_or_prepared_statement, *bound_values)
      #
      # Add a query or a prepared statement to the batch.
      #
      # @example Adding a mix of statements to a batch
      #   batch.add(%(UPDATE people SET name = 'Miriam' WHERE id = 3435))
      #   batch.add(%(UPDATE people SET name = ? WHERE id = ?), 'Miriam', 3435)
      #   batch.add(prepared_statement, 'Miriam', 3435)
      #
      # @param [String, Cql::Client::PreparedStatement] cql_or_prepared_statement
      #   a CQL string or a prepared statement object (obtained through
      #   {Cql::Client::Client#prepare})
      # @param [Array] bound_values a list of bound values -- only applies when
      #   adding prepared statements and when there are binding markers in the
      #   given CQL. If the last argument is a hash and it has the key
      #   `:type_hints` this will be passed as type hints to the request encoder
      #   (if the last argument is any other hash it will be assumed to be a
      #   bound value of type MAP). See {Cql::Client::Client#execute} for more
      #   info on type hints.
      # @return [nil]

      # @!method execute(options={})
      #
      # Execute the batch and return the result.
      #
      # @param options [Hash] an options hash or a symbol (as a shortcut for
      #   specifying the consistency), see {Cql::Client::Client#execute} for
      #   full details about how this value is interpreted.
      # @raise [Cql::QueryError] raised when there is an error on the server side
      # @raise [Cql::NotPreparedError] raised in the unlikely event that a
      #   prepared statement was not prepared on the chosen connection
      # @return [Cql::Client::VoidResult] a batch always returns a void result
    end

    # A convenient wrapper that makes it easy to build batches of multiple
    # executions of the same prepared statement.
    #
    # @see Cql::Client::PreparedStatement#batch
    class PreparedStatementBatch
      # @!method add(*bound_values)
      #
      # Add the statement to the batch with the specified bound values.
      #
      # @param [Array] bound_values the values to bind to the added statement,
      #   see {Cql::Client::PreparedStatement#execute}.
      # @return [nil]

      # @!method execute(options={})
      #
      # Execute the batch and return the result.
      #
      # @raise [Cql::QueryError] raised when there is an error on the server side
      # @raise [Cql::NotPreparedError] raised in the unlikely event that a
      #   prepared statement was not prepared on the chosen connection
      # @return [Cql::Client::VoidResult] a batch always returns a void result
    end

    # @private
    class AsynchronousBatch < Batch
      def initialize(type, execute_options_decoder, connection_manager, options=nil)
        raise ArgumentError, "Unknown batch type: #{type}" unless BATCH_TYPES.include?(type)
        @type = type
        @execute_options_decoder = execute_options_decoder
        @connection_manager = connection_manager
        @options = options
        @request_runner = RequestRunner.new
        @parts = []
      end

      def add(*args)
        @parts << args
        nil
      end

      def execute(options=nil)
        options = @execute_options_decoder.decode_options(@options, options)
        connection = nil
        attempts = 0
        begin
          connection = @connection_manager.random_connection
          request = Protocol::BatchRequest.new(BATCH_TYPES[@type], options[:consistency], options[:trace])
          @parts.each do |cql_or_statement, *bound_args|
            if cql_or_statement.is_a?(String)
              type_hints = nil
              if bound_args.last.is_a?(Hash) && bound_args.last.include?(:type_hints)
                bound_args = bound_args.dup
                type_hints = bound_args.pop[:type_hints]
              end
              request.add_query(cql_or_statement, bound_args, type_hints)
            else
              cql_or_statement.add_to_batch(request, connection, bound_args)
            end
          end
        rescue NotPreparedError
          attempts += 1
          if attempts < 3
            retry
          else
            raise
          end
        end
        @parts = []
        @request_runner.execute(connection, request, options[:timeout])
      end

      private

      BATCH_TYPES = {
        :logged => Protocol::BatchRequest::LOGGED_TYPE,
        :unlogged => Protocol::BatchRequest::UNLOGGED_TYPE,
        :counter => Protocol::BatchRequest::COUNTER_TYPE,
      }.freeze
    end

    # @private
    class SynchronousBatch < Batch
      include SynchronousBacktrace

      def initialize(asynchronous_batch)
        @asynchronous_batch = asynchronous_batch
      end

      def async
        @asynchronous_batch
      end

      def add(*args)
        @asynchronous_batch.add(*args)
      end

      def execute(options=nil)
        synchronous_backtrace { @asynchronous_batch.execute(options).value }
      end
    end

    # @private
    class AsynchronousPreparedStatementBatch < PreparedStatementBatch
      def initialize(prepared_statement, batch)
        @prepared_statement = prepared_statement
        @batch = batch
      end

      def add(*args)
        @batch.add(@prepared_statement, *args)
      end

      def execute(options=nil)
        @batch.execute(options)
      end
    end

    # @private
    class SynchronousPreparedStatementBatch < PreparedStatementBatch
      include SynchronousBacktrace

      def initialize(asynchronous_batch)
        @asynchronous_batch = asynchronous_batch
      end

      def add(*args)
        @asynchronous_batch.add(*args)
      end

      def execute(options=nil)
        synchronous_backtrace { @asynchronous_batch.execute(options).value }
      end
    end
  end
end