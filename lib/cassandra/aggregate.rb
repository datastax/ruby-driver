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
    def initialize(keyspace, name, type, argument_types, state_type, initial_state, state_function, final_function)
      @keyspace       = keyspace
      @name           = name
      @type           = type
      @argument_types = argument_types
      @state_type     = state_type
      @initial_state  = initial_state
      @state_function = state_function
      @final_function = final_function
    end

    def to_cql
      cql = "CREATE AGGREGATE simplex.average("
      first = true
      @argument_types.each do |type|
        if first
          first = false
        else
          cql << ', '
        end
        cql << type.to_s
      end
      cql << ")"
      cql << "\n  SFUNC #{@state_function.name}"
      cql << "\n  STYPE #{@state_type}"
      cql << "\n  FINALFUNC #{@final_function.name}" if @final_function
      cql << "\n  INITCOND #{Util.encode_object(@initial_state)}"
      cql << ";"
    end
  end
end
