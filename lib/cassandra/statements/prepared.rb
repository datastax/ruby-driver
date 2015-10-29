# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
    # A prepared statement is created by calling {Cassandra::Session#prepare}
    # or  {Cassandra::Session#prepare_async}.
    class Prepared
      include Statement

      # @return [String] original cql used to prepare this statement
      attr_reader :cql
      # @private
      attr_reader :result_metadata

      # @private
      def initialize(cql, params_metadata, result_metadata, partition_key, trace_id, keyspace, statement, options, hosts, consistency, retries, client, connection_options)
        @cql                = cql
        @params_metadata    = params_metadata
        @result_metadata    = result_metadata
        @partition_key      = partition_key
        @trace_id           = trace_id
        @keyspace           = keyspace
        @statement          = statement
        @options            = options
        @hosts              = hosts
        @consistency        = consistency
        @retries            = retries
        @client             = client
        @connection_options = connection_options
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

        params = []
        param_types = []

        if args.is_a?(::Hash)
          @params_metadata.each do |(_, _, name, type)|
            name = name.to_sym unless args.has_key?(name)

            if args.has_key?(name)
              value = args[name]
              Util.assert_type(type, value) { "argument for #{name.inspect} must be #{type}, #{value} given" }
              params << value
            elsif @connection_options.protocol_version >= 4
              params << NOT_SET
            else
              raise ::ArgumentError, "argument #{name.inspect} it not present in #{args.inspect}"
            end

            param_types << type
          end
        else
          Util.assert_equal(@params_metadata.size, args.size) { "expecting exactly #{@params_metadata.size} bind parameters, #{args.size} given" }
          @params_metadata.zip(args) do |(_, _, name, type), value|
            Util.assert_type(type, value) { "argument for #{name.inspect} must be #{type}, #{value} given" }
            params << value
            param_types << type
          end
        end

        keyspace_name, _ = @params_metadata.first

        partition_key = create_partition_key(params)

        Bound.new(@cql, param_types, @result_metadata, params, keyspace_name, partition_key)
      end

      # @return [Cassandra::Execution::Info] execution info for PREPARE request
      def execution_info
        @info ||= Execution::Info.new(@keyspace, @statement, @options, @hosts, @consistency, @retries, @trace_id ? Execution::Trace.new(@trace_id, @client) : nil)
      end

      # @return [String] a CLI-friendly prepared statement representation
      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect}>"
      end

      private

      def create_partition_key(values)
        partition_key = @partition_key
        return nil if partition_key.empty? || partition_key.size > values.size
        params_metadata = @params_metadata

        buffer = Protocol::CqlByteBuffer.new
        if partition_key.one?
          i        = partition_key.first
          value    = values[i]
          metadata = params_metadata[i]
          name     = metadata[2]
          type     = metadata[3]

          if value == NOT_SET
            raise ::ArgumentError, "argument #{name.inspect} is a part of " \
                                   "the partition key and must be present."
          end

          if @connection_options.protocol_version >= 3
            Protocol::Coder.write_value_v3(buffer, value, type)
          else
            Protocol::Coder.write_value_v1(buffer, value, type)
          end

          buffer.discard(4) # discard size
        else
          buf = Protocol::CqlByteBuffer.new
          partition_key.each do |i|
            value    = values[i]
            metadata = params_metadata[i]
            name     = metadata[2]
            type     = metadata[3]

            if value == NOT_SET
              raise ::ArgumentError, "argument #{name.inspect} is a part of " \
                                     "the partition key and must be present."
            end

            if @connection_options.protocol_version >= 3
              Protocol::Coder.write_value_v3(buf, value, type)
            else
              Protocol::Coder.write_value_v1(buf, value, type)
            end

            buf.discard(4) # discard size

            size = buf.length
            buffer.append_short(size)
            buffer << buf.read(size) << NULL_BYTE
          end
        end

        buffer.to_str
      end
    end
  end
end
