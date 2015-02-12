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
            ['org.apache.cassandra.db.marshal.AsciiType', [[Types.ascii, :asc]]],
            ['org.apache.cassandra.db.marshal.LongType', [[Types.bigint, :asc]]],
            ['org.apache.cassandra.db.marshal.BytesType', [[Types.blob, :asc]]],
            ['org.apache.cassandra.db.marshal.BooleanType', [[Types.boolean, :asc]]],
            ['org.apache.cassandra.db.marshal.CounterColumnType', [[Types.counter, :asc]]],
            ['org.apache.cassandra.db.marshal.DecimalType', [[Types.decimal, :asc]]],
            ['org.apache.cassandra.db.marshal.DoubleType', [[Types.double, :asc]]],
            ['org.apache.cassandra.db.marshal.FloatType', [[Types.float, :asc]]],
            ['org.apache.cassandra.db.marshal.InetAddressType', [[Types.inet, :asc]]],
            ['org.apache.cassandra.db.marshal.Int32Type', [[Types.int, :asc]]],
            ['org.apache.cassandra.db.marshal.UTF8Type', [[Types.varchar, :asc]]],
            ['org.apache.cassandra.db.marshal.TimestampType', [[Types.timestamp, :asc]]],
            ['org.apache.cassandra.db.marshal.DateType', [[Types.timestamp, :asc]]],
            ['org.apache.cassandra.db.marshal.UUIDType', [[Types.uuid, :asc]]],
            ['org.apache.cassandra.db.marshal.IntegerType', [[Types.varint, :asc]]],
            ['org.apache.cassandra.db.marshal.TimeUUIDType', [[Types.timeuuid, :asc]]],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))', [[Types.varchar, :asc]], {"parameters"=>Types.map(Types.varchar, Types.varchar)}],
            ['org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)', [[Types.map(Types.varchar, Types.varchar), :asc]]],
            ['org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)', [[Types.set(Types.varchar), :asc]]],
            ['org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)', [[Types.list(Types.varchar), :asc]]],
            ['org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.AsciiType)', [[Types.ascii, :desc]]],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type),org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))', [[Types.varchar, :desc]], {"parameters"=>Types.map(Types.varchar, Types.varchar)}],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type))))', [[Types.varchar, :asc]], {"parameters"=>Types.map(Types.varchar, Types.varchar)}]
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
