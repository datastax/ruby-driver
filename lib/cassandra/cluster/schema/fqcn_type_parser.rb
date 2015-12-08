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
  class Cluster
    class Schema
      class FQCNTypeParser
        # @private
        Node   = Struct.new(:parent, :name, :children)
        # @private
        Result = Struct.new(:results, :collections)

        @@types = {
          "org.apache.cassandra.db.marshal.AsciiType"         => :ascii,
          "org.apache.cassandra.db.marshal.LongType"          => :bigint,
          "org.apache.cassandra.db.marshal.BytesType"         => :blob,
          "org.apache.cassandra.db.marshal.BooleanType"       => :boolean,
          "org.apache.cassandra.db.marshal.CounterColumnType" => :counter,
          "org.apache.cassandra.db.marshal.DecimalType"       => :decimal,
          "org.apache.cassandra.db.marshal.DoubleType"        => :double,
          "org.apache.cassandra.db.marshal.FloatType"         => :float,
          "org.apache.cassandra.db.marshal.InetAddressType"   => :inet,
          "org.apache.cassandra.db.marshal.Int32Type"         => :int,
          "org.apache.cassandra.db.marshal.UTF8Type"          => :varchar,
          "org.apache.cassandra.db.marshal.TimestampType"     => :timestamp,
          "org.apache.cassandra.db.marshal.DateType"          => :timestamp,
          "org.apache.cassandra.db.marshal.UUIDType"          => :uuid,
          "org.apache.cassandra.db.marshal.IntegerType"       => :varint,
          "org.apache.cassandra.db.marshal.TimeUUIDType"      => :timeuuid,
          "org.apache.cassandra.db.marshal.MapType"           => :map,
          "org.apache.cassandra.db.marshal.SetType"           => :set,
          "org.apache.cassandra.db.marshal.ListType"          => :list,
          "org.apache.cassandra.db.marshal.UserType"          => :udt,
          "org.apache.cassandra.db.marshal.TupleType"         => :tuple,
          "org.apache.cassandra.db.marshal.ShortType"         => :smallint,
          "org.apache.cassandra.db.marshal.ByteType"          => :tinyint,
          "org.apache.cassandra.db.marshal.TimeType"          => :time,
          "org.apache.cassandra.db.marshal.SimpleDateType"    => :date,
        }.freeze

        def parse(string)
          create_result(parse_node(string))
        end

        private

        def create_result(node)
          collections = nil
          results     = []

          if node.name == "org.apache.cassandra.db.marshal.CompositeType"
            collections = {}

            if node.children.last.name == "org.apache.cassandra.db.marshal.ColumnToCollectionType"
              node.children.pop.children.each do |child|
                key, name  = child.name.split(":")
                key        = [key].pack('H*').force_encoding(::Encoding::UTF_8)

                if name == "org.apache.cassandra.db.marshal.ReversedType"
                  collections[key] = lookup_type(child.children.first)
                else
                  child.name = name
                  collections[key] = lookup_type(child)
                end
              end
            end

            node.children.each do |child|
              results << create_type(child)
            end
          else
            results << create_type(node)
          end

          Result.new(results, collections)
        end

        def create_type(node)
          order  = :asc
          frozen = false

          if node.name == "org.apache.cassandra.db.marshal.ReversedType"
            order = :desc
            node  = node.children.first
          end

          if node.name == "org.apache.cassandra.db.marshal.FrozenType"
            frozen = true
            node   = node.children.first
          end

          [lookup_type(node), order, frozen]
        end

        def lookup_type(node)
          if node.name == "org.apache.cassandra.db.marshal.FrozenType"
            return lookup_type(node.children.first)
          end

          type = @@types.fetch(node.name) { return Cassandra::Types.custom(dump_node(node)) }

          case type
          when :set, :list
            Cassandra::Types.send(type, lookup_type(node.children.first))
          when :map
            Cassandra::Types.map(*node.children.map(&method(:lookup_type)))
          when :udt
            keyspace = node.children.shift.name
            name     = [node.children.shift.name].pack('H*')
            fields   = node.children.map do |child|
              field_name, child_name = child.name.split(":")

              child.name = child_name
              field_name = [field_name].pack('H*').force_encoding(::Encoding::UTF_8)

              [field_name, lookup_type(child)]
            end

            Cassandra::Types.udt(keyspace, name, fields)
          when :tuple
            Cassandra::Types.tuple(*node.children.map(&method(:lookup_type)))
          else
            Cassandra::Types.send(type)
          end
        end

        def parse_node(string)
          root = node = Node.new(nil, '', [])

          string.each_char do |char|
            case char
            when '(' # starting type params
              child = Node.new(node, '', [])
              node.children << child
              node = child
            when ','
              child = Node.new(node.parent, '', [])
              node.parent.children << child
              node = child
            when ')'
              node = node.parent
            when ' '
              next
            else
              node.name << char
            end
          end

          root
        end

        def dump_node(node)
          str = node.name
          str << '(' + node.children.map {|n| dump_node(n)}.join(',') + ')' unless node.children.empty?
          str
        end
      end
    end
  end
end
