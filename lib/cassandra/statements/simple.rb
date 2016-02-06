# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
    class Simple
      include Statement

      # @return [String] original cql used to prepare this statement
      attr_reader :cql
      # @return [Array<Object>] a list of positional parameters for the cql
      attr_reader :params

      # @private
      attr_reader :params_types

      # @private
      attr_reader :params_names

      # @param cql [String] a cql statement
      # @param params [Array, Hash] (nil) positional or named arguments
      #   for the query
      # @param type_hints [Array, Hash] (nil) positional or named types
      #   to override type guessing for the query
      # @param idempotent [Boolean] (false) whether this statement can be
      #   safely retries on timeouts
      #
      # @note Positional arguments for simple statements are only supported
      #   starting with Apache Cassandra 2.0 and above.
      #
      # @note Named arguments for simple statements are only supported
      #   starting with Apache Cassandra 2.1 and above.
      #
      # @raise [ArgumentError] if cql statement given is not a String
      def initialize(cql, params = nil, type_hints = nil, idempotent = false)
        Util.assert_instance_of(::String, cql) do
          "cql must be a string, #{cql.inspect} given"
        end

        params ||= EMPTY_LIST

        if params.is_a?(::Hash)
          params_names = []
          params = params.each_with_object([]) do |(name, value), collector|
            params_names << name
            collector    << value
          end
          if type_hints && !type_hints.empty?
            Util.assert_instance_of(::Hash, type_hints) do
              'type_hints must be a Hash when using named params'
            end
          end
        else
          Util.assert_instance_of(::Array, params) do
            "params must be an Array or a Hash, #{params.inspect} given"
          end
          params_names = EMPTY_LIST
        end

        type_hints ||= EMPTY_LIST

        if type_hints.is_a?(::Hash)
          type_hints = params_names.map {|name| type_hints[name] }
        else
          Util.assert_instance_of(::Array, type_hints) do
            "type_hints must be an Array or a Hash, #{type_hints.inspect} given"
          end
        end

        @cql          = cql
        @params       = params
        @params_types = params.each_with_index.map do |value, index|
          (!type_hints.empty? && type_hints[index] && type_hints[index].is_a?(Type)) ?
              type_hints[index] :
              Util.guess_type(value)
        end
        @params_names = params_names
        @idempotent   = idempotent
      end

      # @return [String] a CLI-friendly simple statement representation
      def inspect
        "#<#{self.class.name}:0x#{object_id.to_s(16)} @cql=#{@cql.inspect} " \
            "@params=#{@params.inspect}>"
      end

      # @param other [Object, Cassandra::Statements::Simple] object to compare
      # @return [Boolean] whether the statements are equal
      def eql?(other)
        other.is_a?(Simple) &&
          @cql == other.cql &&
          @params == other.params
      end

      alias == eql?
    end
  end
end
