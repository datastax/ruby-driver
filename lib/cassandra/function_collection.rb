# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
  # @private
  # This class encapsulates a collection of functions or aggregates.
  # Really used internally, so it should not be documented.
  class FunctionCollection
    def initialize
      @function_hash = {}
    end

    # Get the Function or Aggregate with the given name and argument-types.
    # @param name [String] the name of the function/aggregate.
    # @param argument_types [Array<Cassandra::Type>] list of argument-types.
    # @return [Cassandra::Function] or [Cassandra::Aggregate] if found;
    #     nil otherwise.
    def get(name, argument_types)
      @function_hash[[name, argument_types]]
    end

    def add_or_update(function)
      @function_hash[[function.name, function.argument_types]] = function
    end

    def delete(name, argument_types)
      @function_hash.delete([name, argument_types])
    end

    # @return [Boolean] whether this FunctionCollection is equal to the other
    def eql?(other)
      other.is_a?(FunctionCollection) &&
        @function_hash == other.raw_functions
    end
    alias == eql?

    def hash
      @function_hash.hash
    end

    # Yield or enumerate each function defined in this collection
    # @overload each_function
    #   @yieldparam function [Cassandra::Function or Cassandra::Aggregate]
    #         current function or aggregate
    #   @return [Cassandra::FunctionCollection] self
    # @overload each_function
    #   @return [Array<Cassandra::Function> or Array<Cassandra::Aggregate>]
    #       a list of functions or aggregates.
    def each_function(&block)
      if block_given?
        @function_hash.each_value(&block)
        self
      else
        @function_hash.values
      end
    end
    alias functions each_function

    def inspect
      "#<Cassandra::FunctionCollection:0x#{object_id.to_s(16)} " \
          "@function_hash=#{@function_hash.inspect}>"
    end

    protected

    def raw_functions
      @function_hash
    end
  end
end
