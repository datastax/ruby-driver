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
  class Cluster
    class Schema
      class CQLTypeParser
        IncompleteTypeError = ::Class.new(::StandardError)

        # @private
        Node = Struct.new(:parent, :name, :children)

        def parse(string, types)
          frozen = false
          node   = parse_node(string)

          if node.name == 'frozen'
            frozen = true
            node   = node.children.first
          end

          [lookup_type(node, types), frozen]
        end

        private

        def lookup_type(node, types)
          return lookup_type(node.children.first, types) if node.name == 'frozen'

          case node.name
          when 'text'              then Cassandra::Types.text
          when 'blob'              then Cassandra::Types.blob
          when 'ascii'             then Cassandra::Types.ascii
          when 'bigint'            then Cassandra::Types.bigint
          when 'counter'           then Cassandra::Types.counter
          when 'int'               then Cassandra::Types.int
          when 'varint'            then Cassandra::Types.varint
          when 'boolean'           then Cassandra::Types.boolean
          when 'decimal'           then Cassandra::Types.decimal
          when 'double'            then Cassandra::Types.double
          when 'float'             then Cassandra::Types.float
          when 'inet'              then Cassandra::Types.inet
          when 'timestamp'         then Cassandra::Types.timestamp
          when 'uuid'              then Cassandra::Types.uuid
          when 'timeuuid'          then Cassandra::Types.timeuuid
          when 'date'              then Cassandra::Types.date
          when 'smallint'          then Cassandra::Types.smallint
          when 'time'              then Cassandra::Types.time
          when 'tinyint'           then Cassandra::Types.tinyint
          when 'map'               then
            Cassandra::Types.map(*node.children.map { |t| lookup_type(t, types)})
          when 'set'               then
            Cassandra::Types.set(lookup_type(node.children.first, types))
          when 'list'              then
            Cassandra::Types.list(lookup_type(node.children.first, types))
          when 'tuple'             then
            Cassandra::Types.tuple(*node.children.map { |t| lookup_type(t, types)})
          when 'empty'             then
            Cassandra::Types.custom('org.apache.cassandra.db.marshal.EmptyType')
          when /\A'/ then
            # Custom type.
            Cassandra::Types.custom(node.name[1..-2])
          else
            types.fetch(node.name) do
              raise IncompleteTypeError, "unable to lookup type #{node.name.inspect}"
            end
          end
        end

        def parse_node(string)
          root = node = Node.new(nil, '', [])

          string.each_char do |char|
            case char
            when '<' # starting type params
              child = Node.new(node, '', [])
              node.children << child
              node = child
            when ','
              child = Node.new(node.parent, '', [])
              node.parent.children << child
              node = child
            when '>'
              node = node.parent
            when ' '
              next
            else
              node.name << char unless char == '"'
            end
          end

          root
        end
      end
    end
  end
end
