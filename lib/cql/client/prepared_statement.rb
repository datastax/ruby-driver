# encoding: utf-8

module Cql
  module Client
    class AsynchronousPreparedStatement
      # @return [ResultMetadata]
      attr_reader :metadata

      def initialize(*args)
        @client, @connection_id, @statement_id, @raw_metadata = args
        @metadata = ResultMetadata.new(@raw_metadata)
      end

      # Execute the prepared statement with a list of values for the bound parameters.
      #
      # The number of arguments must equal the number of bound parameters.
      # To set the consistency level for the request you pass a consistency
      # level (as a symbol) as the last argument. Needless to say, if you pass
      # the value for one bound parameter too few, and then a consistency level,
      # or if you pass too many values, you will get weird errors.
      #
      # @param args [Array] the values for the bound parameters, and optionally
      #   the desired consistency level, as a symbol (defaults to :quorum)
      #
      def execute(*args)
        bound_args = args.shift(@raw_metadata.size)
        consistency_level = args.shift
        @client.execute_statement(@connection_id, @statement_id, @raw_metadata, bound_args, consistency_level)
      end
    end

    class SynchronousPreparedStatement
      def initialize(async_statement)
        @async_statement = async_statement
      end

      def metadata
        @async_statement.metadata
      end

      def execute(*args)
        @async_statement.execute(*args).get
      end
    end
  end
end