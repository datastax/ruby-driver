# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
  # Represents a cassandra user defined aggregate
  # @see Cassandra::Keyspace#each_aggregate
  # @see Cassandra::Keyspace#aggregate
  # @see Cassandra::Keyspace#has_aggregate?
  class Aggregate
    # @private
    attr_reader :keyspace
    # @return [String] aggregate name
    attr_reader :name
    # @return [Cassandra::Type] aggregate return type
    attr_reader :type
    # @return [Array<Cassandra::Type>] aggregate argument types
    attr_reader :argument_types
    # @return [Cassandra::Type] aggregate state type
    attr_reader :state_type
    # @return [Object, nil] the initial value of the aggregate state or nil
    attr_reader :initial_state
    # @return [Cassandra::Function] the state function used by this aggregate
    attr_reader :state_function
    # @return [Cassandra::Function] the final function used by this aggregate
    attr_reader :final_function

    # @private
    def initialize(keyspace,
                   name,
                   type,
                   argument_types,
                   state_type,
                   initial_state,
                   state_function,
                   final_function)
      @keyspace       = keyspace
      @name           = name
      @type           = type
      @argument_types = argument_types
      @state_type     = state_type
      @initial_state  = initial_state
      @state_function = state_function
      @final_function = final_function
    end

    # @private
    def eql?(other)
      other.is_a?(Aggregate) && \
        @keyspace == other.keyspace && \
        @name == other.name && \
        @type == other.type && \
        @argument_types == other.argument_types && \
        @state_type == other.state_type && \
        @initial_state == other.initial_state && \
        @state_function == other.state_function && \
        @final_function == other.final_function
    end
    alias == eql?

    # @private
    def hash
      @hash ||= begin
        h = 17
        h = 31 * h + @keyspace.hash
        h = 31 * h + @name.hash
        h = 31 * h + @type.hash
        h = 31 * h + @argument_types.hash
        h = 31 * h + @state_type.hash
        h = 31 * h + @initial_state.hash
        h = 31 * h + @state_function.hash
        h = 31 * h + @final_function.hash
        h
      end
    end

    # @private
    def inspect
      "#<Cassandra::Aggregate:0x#{object_id.to_s(16)} " \
          "@keyspace=#{@keyspace.inspect}, " \
          "@name=#{@name.inspect}, " \
          "@type=#{@type.inspect}, " \
          "@argument_types=#{@argument_types.inspect}, " \
          "@initial_state=#{@initial_state.inspect}, " \
          "@state_function=#{@state_function.inspect}, " \
          "@final_function=#{@final_function.inspect}>"
    end

    # @return [String] a cql representation of this aggregate
    def to_cql
      cql = 'CREATE AGGREGATE simplex.average('
      first = true
      @argument_types.each do |type|
        if first
          first = false
        else
          cql << ', '
        end
        cql << type.to_s
      end
      cql << ')'
      cql << "\n  SFUNC #{@state_function.name}"
      cql << "\n  STYPE #{@state_type}"
      cql << "\n  FINALFUNC #{@final_function.name}" if @final_function
      cql << "\n  INITCOND #{@initial_state}"
      cql << ';'
    end
  end
end
