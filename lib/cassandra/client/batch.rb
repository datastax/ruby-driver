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

module Cassandra
  module Client
    # Batches let you send multiple queries (`INSERT`, `UPDATE` and `DELETE`) in
    # one go. This can lead to better performance, and depending on the options
    # you specify can also give you different consistency guarantees.
    #
    # Batches can contain a mix of different queries and prepared statements.
    #
    # @see Cassandra::Client::Client#batch
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
      # @param [String, Cassandra::Client::PreparedStatement] cql_or_prepared_statement
      #   a CQL string or a prepared statement object (obtained through
      #   {Cassandra::Client::Client#prepare})
      # @param [Array] bound_values a list of bound values -- only applies when
      #   adding prepared statements and when there are binding markers in the
      #   given CQL. If the last argument is a hash and it has the key
      #   `:type_hints` this will be passed as type hints to the request encoder
      #   (if the last argument is any other hash it will be assumed to be a
      #   bound value of type MAP). See {Cassandra::Client::Client#execute} for more
      #   info on type hints.
      # @return [nil]

      # @!method execute(options={})
      #
      # Execute the batch and return the result.
      #
      # @param [Hash] options an options hash or a symbol (as a shortcut for
      #   specifying the consistency), see {Cassandra::Client::Client#execute} for
      #   full details about how this value is interpreted.
      # @raise [Cassandra::Errors::QueryError] raised when there is an error on the server side
      # @raise [Cassandra::Errors::NotPreparedError] raised in the unlikely event that a
      #   prepared statement was not prepared on the chosen connection
      # @return [Cassandra::Client::VoidResult] a batch always returns a void result
    end

    # A convenient wrapper that makes it easy to build batches of multiple
    # executions of the same prepared statement.
    #
    # @see Cassandra::Client::PreparedStatement#batch
    class PreparedStatementBatch
      # @!method add(*bound_values)
      #
      # Add the statement to the batch with the specified bound values.
      #
      # @param [Array] bound_values the values to bind to the added statement,
      #   see {Cassandra::Client::PreparedStatement#execute}.
      # @return [nil]

      # @!method execute(options={})
      #
      # Execute the batch and return the result.
      #
      # @raise [Cassandra::Errors::QueryError] raised when there is an error on the server side
      # @raise [Cassandra::Errors::NotPreparedError] raised in the unlikely event that a
      #   prepared statement was not prepared on the chosen connection
      # @return [Cassandra::Client::VoidResult] a batch always returns a void result
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
        connection = @connection_manager.random_connection
        request = Protocol::BatchRequest.new(BATCH_TYPES[@type], options[:consistency], options[:trace])
        unprepared_statements = nil
        @parts.each do |part, *bound_args|
          if part.is_a?(String) || part.prepared?(connection)
            add_part(connection, request, part, bound_args)
          else
            unprepared_statements ||= []
            unprepared_statements << [part, bound_args]
          end
        end
        @parts = []
        if unprepared_statements.nil?
          @request_runner.execute(connection, request, options[:timeout])
        else
          fs = unprepared_statements.map do |statement, _|
            if statement.respond_to?(:async)
              statement.async.prepare(connection)
            else
              statement.prepare(connection)
            end
          end
          Ione::Future.all(*fs).flat_map do
            unprepared_statements.each do |statement, bound_args|
              add_part(connection, request, statement, bound_args)
            end
            @request_runner.execute(connection, request, options[:timeout])
          end
        end
      end

      private

      BATCH_TYPES = {
        :logged => Protocol::BatchRequest::LOGGED_TYPE,
        :unlogged => Protocol::BatchRequest::UNLOGGED_TYPE,
        :counter => Protocol::BatchRequest::COUNTER_TYPE,
      }.freeze

      def add_part(connection, request, part, bound_args)
        if part.is_a?(String)
          type_hints = nil
          if bound_args.last.is_a?(Hash) && bound_args.last.include?(:type_hints)
            bound_args = bound_args.dup
            type_hints = bound_args.pop[:type_hints]
          end
          request.add_query(part, bound_args, type_hints)
        else
          part.add_to_batch(request, connection, bound_args)
        end
      end
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