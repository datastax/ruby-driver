# encoding: utf-8

require 'spec_helper'


module Cql
  module Protocol
    describe SchemaChangeResultResponse do
      describe '.decode' do
        let :response do
          buffer = CqlByteBuffer.new("\x00\aUPDATED\x00\ncql_rb_973\x00\x05users")
          described_class.decode(1, buffer, buffer.length)
        end

        it 'decodes the description' do
          response.change.should == 'UPDATED'
        end

        it 'decodes the keyspace' do
          response.keyspace.should == 'cql_rb_973'
        end

        it 'decodes the table' do
          response.table.should == 'users'
        end
      end

      describe '#void?' do
        it 'is not void' do
          response = described_class.new('CREATED', 'ks', 'tbl', nil)
          response.should_not be_void
        end
      end

      describe '#to_s' do
        it 'returns a string with the description, keyspace and table' do
          response = described_class.new('CREATED', 'ks', 'tbl', nil)
          response.to_s.should == 'RESULT SCHEMA_CHANGE CREATED "ks" "tbl"'
        end
      end

      describe '#eql?' do
        it 'is equal to another response with the same change, keyspace and table names' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response1.should eql(response2)
        end

        it 'is not equal to another response with another change' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('CREATED', 'some_keyspace', 'a_table', nil)
          response1.should_not eql(response2)
        end

        it 'is not equal to another response with another keyspace name' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('DROPPED', 'another_keyspace', 'a_table', nil)
          response1.should_not eql(response2)
        end

        it 'is not equal to another response with another table name' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('DROPPED', 'some_keyspace', 'another_table', nil)
          response1.should_not eql(response2)
        end

        it 'is aliased as ==' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response3 = described_class.new('DROPPED', 'some_keyspace', 'another_table', nil)
          response1.should == response2
          response2.should_not == response3
        end
      end

      describe '#hash' do
        it 'is the same when the change, keyspace and table names are the same' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response1.hash.should == response2.hash
        end

        it 'is not the same when the change is different' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('CREATED', 'some_keyspace', 'a_table', nil)
          response1.hash.should_not == response2.hash
        end

        it 'is not the same when the keyspace name is different' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('DROPPED', 'another_keyspace', 'a_table', nil)
          response1.hash.should_not == response2.hash
        end

        it 'is not the same when the table name is different' do
          response1 = described_class.new('DROPPED', 'some_keyspace', 'a_table', nil)
          response2 = described_class.new('DROPPED', 'some_keyspace', 'another_table', nil)
          response1.hash.should_not == response2.hash
        end
      end
    end
  end
end
