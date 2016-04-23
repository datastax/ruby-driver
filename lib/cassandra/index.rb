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
  # Represents an index on a cassandra table
  class Index
    # @return [Cassandra::Table] table that the index applies to.
    attr_reader :table
    # @return [String] name of the index.
    attr_reader :name
    # @return [Symbol] kind of index: `:keys`, `:composites`, or `:custom`.
    attr_reader :kind
    # @return [String] name of column that the index applies to.
    attr_reader :target
    # @return [Hash] options of the index.
    attr_reader :options

    # @private
    def initialize(table,
                   name,
                   kind,
                   target,
                   options)
      @table = table
      @name = name.freeze
      @kind = kind
      @options = options.freeze

      # Target is a bit tricky; it may be an escaped name or not
      # depending on C* version. Unify to be unescaped since a user
      # who wants to know the target would want the bare column name.

      @target = if target[0] == '"'
                  target[1..-2]
                else
                  target
                end.freeze
    end

    # @return [Boolean] whether or not this index uses a custom class.
    def custom_index?
      !@options['class_name'].nil?
    end

    # @return [String] name of the index class if this is a custom index; nil otherwise.
    def custom_class_name
      @options['class_name']
    end

    # @return [String] a cql representation of this table
    def to_cql
      keyspace_name = Util.escape_name(@table.keyspace.name)
      table_name = Util.escape_name(@table.name)
      index_name = Util.escape_name(@name)
      column_name = Util.escape_name(@target)

      if custom_index?
        "CREATE CUSTOM INDEX #{index_name} ON #{keyspace_name}.#{table_name} (#{column_name}) " \
        "USING '#{@options['class_name']}'#{options_cql};"
      else
        "CREATE INDEX #{index_name} ON #{keyspace_name}.#{table_name} (#{column_name});"
      end
    end

    # @private
    def eql?(other)
      other.is_a?(Index) &&
        @table == other.table &&
        @name == other.name &&
        @kind == other.kind &&
        @target == other.target &&
        @options == other.options
    end
    alias == eql?

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "@name=#{@name.inspect} @table=#{@table.inspect} @kind=#{@kind.inspect} @target=#{@target.inspect}>"
    end

    private

    def options_cql
      # exclude 'class_name', 'target' keys
      filtered_options = @options.reject do |key, _|
        key == 'class_name' || key == 'target'
      end
      return '' if filtered_options.empty?

      result = ' WITH OPTIONS = {'
      result << filtered_options.map do |key, value|
        "'#{key}': '#{value}'"
      end.join(', ')
      result << '}'
      result
    end
  end
end
