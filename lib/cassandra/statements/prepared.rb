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

module Cassandra
  module Statements
    class Prepared
      include Statement

      # @return [String] original cql used to prepare this statement
      attr_reader :cql
      # @private
      attr_reader :result_metadata

      # @private
      def initialize(cql, params_metadata, result_metadata, trace_id, keyspace, statement, options, hosts, consistency, retries, client, futures_factory, schema)
        @cql             = cql
        @params_metadata = params_metadata
        @result_metadata = result_metadata
        @trace_id        = trace_id
        @keyspace        = keyspace
        @statement       = statement
        @options         = options
        @hosts           = hosts
        @consistency     = consistency
        @retries         = retries
        @client          = client
        @schema          = schema
      end

      # Creates a statement bound with specific arguments
      #
      # @param args [Array, Hash] (nil) positional or named arguments to bind,
      #   must contain the same number of parameters as the number of positional
      #   (`?`) or named (`:name`) markers in the original CQL passed to
      #   {Cassandra::Session#prepare}
      #
      # @return [Cassandra::Statements::Bound] bound statement
      def bind(args = nil)
        if args
          Util.assert_instance_of_one_of([::Array, ::Hash], args) { "args must be an Array or a Hash, #{args.inspect} given" }
        else
          args = EMPTY_LIST
        end

        if args.is_a?(Hash)
          args = @params_metadata.map do |(_, _, name, type)|
            unless args.has_key?(name)
              name = name.to_sym
              raise ::ArgumentError, "argument :#{name} must be present in #{args.inspect}, but isn't" unless args.has_key?(name)
            end

            args[name]
          end
        else
          Util.assert_equal(@params_metadata.size, args.size) { "expecting exactly #{@params_metadata.size} bind parameters, #{args.size} given" }
        end

        params_types = @params_metadata.each_with_index.map do |(_, _, name, type), i|
          Util.assert_type(type, args[i]) { "argument for #{name.inspect} must be #{Util.type_to_cql(type).inspect}, #{args[i]} given" }
          type
        end

        return Bound.new(@cql, params_types, @result_metadata, args) if @params_metadata.empty?

        keyspace, table, _, _ = @params_metadata.first
        return Bound.new(@cql, params_types, @result_metadata, args, keyspace) unless keyspace && table

        values = ::Hash.new
        @params_metadata.zip(args) do |(keyspace, table, column, type), value|
          values[column] = value
        end

        partition_key = @schema.create_partition_key(keyspace, table, values)

        Bound.new(@cql, params_types, @result_metadata, args, keyspace, partition_key)
      end

      # @return [Cassandra::Execution::Info] execution info for PREPARE request
      def execution_info
        @info ||= Execution::Info.new(@keyspace, @statement, @options, @hosts, @consistency, @retries, @trace_id ? Execution::Trace.new(@trace_id, @client) : nil)
      end

      # @return [String] a CLI-friendly prepared statement representation
      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect}>"
      end
    end
  end
end
