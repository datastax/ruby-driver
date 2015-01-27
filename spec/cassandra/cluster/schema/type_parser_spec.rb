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

require 'spec_helper'

module Cassandra
  class Cluster
    class Schema
      describe(TypeParser) do
        let(:parser) { TypeParser.new }

        describe('#parse') do
          [
            ['org.apache.cassandra.db.marshal.AsciiType', [[:ascii, :asc]]],
            ['org.apache.cassandra.db.marshal.LongType', [[:bigint, :asc]]],
            ['org.apache.cassandra.db.marshal.BytesType', [[:blob, :asc]]],
            ['org.apache.cassandra.db.marshal.BooleanType', [[:boolean, :asc]]],
            ['org.apache.cassandra.db.marshal.CounterColumnType', [[:counter, :asc]]],
            ['org.apache.cassandra.db.marshal.DecimalType', [[:decimal, :asc]]],
            ['org.apache.cassandra.db.marshal.DoubleType', [[:double, :asc]]],
            ['org.apache.cassandra.db.marshal.FloatType', [[:float, :asc]]],
            ['org.apache.cassandra.db.marshal.InetAddressType', [[:inet, :asc]]],
            ['org.apache.cassandra.db.marshal.Int32Type', [[:int, :asc]]],
            ['org.apache.cassandra.db.marshal.UTF8Type', [[:varchar, :asc]]],
            ['org.apache.cassandra.db.marshal.TimestampType', [[:timestamp, :asc]]],
            ['org.apache.cassandra.db.marshal.DateType', [[:timestamp, :asc]]],
            ['org.apache.cassandra.db.marshal.UUIDType', [[:uuid, :asc]]],
            ['org.apache.cassandra.db.marshal.IntegerType', [[:varint, :asc]]],
            ['org.apache.cassandra.db.marshal.TimeUUIDType', [[:timeuuid, :asc]]],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))', [[:varchar, :asc]], {"parameters"=>[:map, :varchar, :varchar]}],
            ['org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)', [[[:map, :varchar, :varchar], :asc]]],
            ['org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)', [[[:set, :varchar], :asc]]],
            ['org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)', [[[:list, :varchar], :asc]]],
            ['org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.AsciiType)', [[:ascii, :desc]]],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type),org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))', [[:varchar, :desc]], {"parameters"=>[:map, :varchar, :varchar]}],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type))))', [[:varchar, :asc]], {"parameters"=>[:map, :varchar, :varchar]}]
          ].each do |cassandra_type, results, collections|
            it "parses #{cassandra_type.inspect} as results=#{results.inspect} collections=#{collections.inspect}" do
              result = parser.parse(cassandra_type)
              expect(result.collections).to eq(collections)
              expect(result.results).to eq(results)
            end
          end
        end
      end
    end
  end
end
