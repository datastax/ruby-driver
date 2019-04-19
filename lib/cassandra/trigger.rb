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
  # Represents a trigger on a cassandra table
  class Trigger
    # @return [Cassandra::Table] table that the trigger applies to.
    attr_reader :table
    # @return [String] name of the trigger.
    attr_reader :name
    # @return [Hash] options of the trigger.
    attr_reader :options

    # @private
    def initialize(table,
                   name,
                   options)
      @table = table
      @name = name.freeze
      @options = options.freeze
    end

    # @return [String] name of the trigger class
    def custom_class_name
      @options['class']
    end

    # @return [String] a cql representation of this trigger
    def to_cql
      keyspace_name = Util.escape_name(@table.keyspace.name)
      table_name = Util.escape_name(@table.name)
      trigger_name = Util.escape_name(@name)

      "CREATE TRIGGER #{trigger_name} ON #{keyspace_name}.#{table_name} USING '#{@options['class']}';"
    end

    # @private
    def eql?(other)
      other.is_a?(Trigger) &&
        @table == other.table &&
        @name == other.name &&
        @options == other.options
    end
    alias == eql?

    # @private
    def inspect
      "#<#{self.class.name}:0x#{object_id.to_s(16)} " \
          "@name=#{@name.inspect} @table=#{@table.inspect} @options=#{@options.inspect}>"
    end
  end
end
