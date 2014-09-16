# encoding: utf-8

require 'spec_helper'


module Cassandra
  module Protocol
    describe CustomTypeParser do
      let :parser do
        described_class.new
      end

      describe '#type' do
        it 'maps internal type names to protocol type names' do
          type = parser.parse_type('org.apache.cassandra.db.marshal.DecimalType')
          type.should == :decimal
          type = parser.parse_type('org.apache.cassandra.db.marshal.LongType')
          type.should == :bigint
        end

        it 'returns custom types as-is' do
          type = parser.parse_type('com.acme.Foo')
          type.should == [:custom, 'com.acme.Foo']
        end

        it 'parses flat user defined types' do
          type = parser.parse_type('org.apache.cassandra.db.marshal.UserType(some_keyspace,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a6970:org.apache.cassandra.db.marshal.Int32Type)')
          type.should == [:udt, {'street' => :text, 'city' => :text, 'zip' => :int}]
        end

        it 'parses nested user defined types' do
          type = parser.parse_type('org.apache.cassandra.db.marshal.UserType(some_keyspace,636f6d70616e79,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616464726573736573:org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UserType(cql_rb_client_spec,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a6970:org.apache.cassandra.db.marshal.Int32Type)))')
          type.should == [:udt, {'name' => :text, 'addresses' => [:list, [:udt, {'street' => :text, 'city' => :text, 'zip' => :int}]]}]
        end

        it 'parses nested user defined types where the inner UDT is a map key' do
          type = parser.parse_type('org.apache.cassandra.db.marshal.UserType(some_keyspace,636f6d70616e79,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616464726573736573:org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UserType(cql_rb_client_spec,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a6970:org.apache.cassandra.db.marshal.Int32Type),org.apache.cassandra.db.marshal.Int32Type))')
          type.should == [:udt, {'name' => :text, 'addresses' => [:map, [:udt, {'street' => :text, 'city' => :text, 'zip' => :int}], :int]}]
        end
      end
    end
  end
end
