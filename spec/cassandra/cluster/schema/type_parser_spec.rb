# encoding: utf-8

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
            ['org.apache.cassandra.db.marshal.UTF8Type', [[:text, :asc]]],
            ['org.apache.cassandra.db.marshal.TimestampType', [[:timestamp, :asc]]],
            ['org.apache.cassandra.db.marshal.DateType', [[:timestamp, :asc]]],
            ['org.apache.cassandra.db.marshal.UUIDType', [[:uuid, :asc]]],
            ['org.apache.cassandra.db.marshal.IntegerType', [[:varint, :asc]]],
            ['org.apache.cassandra.db.marshal.TimeUUIDType', [[:timeuuid, :asc]]],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))', [[:text, :asc]], {"parameters"=>[:map, :text, :text]}],
            ['org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)', [[[:map, :text, :text], :asc]]],
            ['org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)', [[[:set, :text], :asc]]],
            ['org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)', [[[:list, :text], :asc]]],
            ['org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.AsciiType)', [[:ascii, :desc]]],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.UTF8Type),org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)))', [[:text, :desc]], {"parameters"=>[:map, :text, :text]}],
            ['org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ColumnToCollectionType(706172616d6574657273:org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type))))', [[:text, :asc]], {"parameters"=>[:map, :text, :text]}]
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
