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
    class Simple
      include Statement

      # @return [String] original cql used to prepare this statement
      attr_reader :cql
      # @return [Array<Object>] a list of positional parameters for the cql
      attr_reader :params

      # @private
      attr_reader :params_types

      # @param cql [String] a cql statement
      # @param params [Array] (nil) positional arguments for the query
      #
      # @note Positional arguments for simple statements are only supported
      #   starting with Apache Cassandra 2.0 and above.
      #
      # @raise [ArgumentError] if cql statement given is not a String
      def initialize(cql, params = nil)
        Util.assert_instance_of(::String, cql) { "cql must be a string, #{cql.inspect} given" }

        if params
          Util.assert_instance_of(::Array, params) { "params must be an Array, #{params.inspect} given" }
        else
          params = EMPTY_LIST
        end

        params_types = params.map {|value| Util.guess_type(value)}

        @cql          = cql
        @params       = params
        @params_types = params_types
      end

      # @return [String] a CLI-friendly simple statement representation
      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @cql=#{@cql.inspect} @params=#{@params.inspect}>"
      end

      # @param other [Object, Cassandra::Statements::Simple] object to compare
      # @return [Boolean] whether the statements are equal
      def eql?(other)
        other.is_a?(Simple) &&
          @cql == other.cql &&
          @params == other.params
      end

      alias :== :eql?
    end
  end
end
