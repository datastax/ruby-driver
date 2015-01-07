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
  # Represents a cassandra column
  # @see Cassandra::Table#each_column
  # @see Cassandra::Table#column
  class Column
    # @private
    class Index
      # @return [String] index name
      attr_reader :name
      # @return [String] custom index class name
      attr_reader :custom_class_name

      # @private
      def initialize(name, custom_class_name = nil)
        @name              = name
        @custom_class_name = custom_class_name
      end
    end

    # @return [String] column name
    attr_reader :name
    # @private
    # @return [Symbol, Array(Symbol, Symbol)] column type
    attr_reader :type
    # @return [Symbol] column order (`:asc` or `:desc`)
    attr_reader :order
    # @private
    # @return [Cassandra::Column::Index, nil] column index
    attr_reader :index
    # @private
    # @return [Boolean] whether the column is static
    attr_reader :static

    # @private
    def initialize(name, type, order, index = nil, is_static = false)
      @name     = name
      @type     = type
      @order    = order
      @index    = index
      @static   = is_static
    end

    # @return [Boolean] whether the column is static
    def static?
      @static
    end

    # @return [String] a cql representation of this column
    def to_cql
      cql = "#{@name} #{Util.type_to_cql(@type)}"
      cql << ' static' if @static
      cql
    end

    # @return [String] a CLI-friendly column representation
    def inspect
      "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @name=#{@name}>"
    end

    # @return [Boolean] whether this column is equal to the other
    def eql?(other)
      other.is_a?(Column) &&
        @name == other.name &&
        @type == other.type &&
        @order == other.order &&
        @index == other.index &&
        @static == other.static?
    end
    alias :== :eql?
  end
end
