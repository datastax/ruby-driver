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
  # Represents a cassandra column
  # @see Cassandra::Table#each_column
  # @see Cassandra::Table#column
  class Column
    # @return [String] column name
    attr_reader :name
    # @return [Cassandra::Type] column type
    attr_reader :type
    # @return [Symbol] column order (`:asc` or `:desc`)
    attr_reader :order

    # @private
    def initialize(name, type, order, is_static = false, is_frozen = false)
      @name     = name
      @type     = type
      @order    = order
      @static   = is_static
      @frozen   = is_frozen
    end

    # @return [Boolean] whether the column is static
    def static?
      @static
    end

    # @return [Boolean] whether the column is frozen
    def frozen?
      @frozen
    end

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} @name=#{@name} @type=#{@type}>"
    end

    # @private
    def eql?(other)
      other.is_a?(Column) &&
        @name == other.name &&
        @type == other.type &&
        @order == other.order &&
        @static == other.static? &&
        @frozen == other.frozen?
    end
    alias == eql?
  end
end
