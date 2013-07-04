# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe ColumnMetadata do
      describe '#eql?' do
        it 'is equal to another column metadata with the same properties' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm1.should eql(cm2)
        end

        it 'is aliased as #==' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm1.should == cm2
        end

        it 'is not equal to another column metadata when the keyspace names differ' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('another_keyspace', 'my_table', 'my_column', :uuid)
          cm1.should_not eql(cm2)
        end

        it 'is not equal to another column metadata when the table names differ' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'another_table', 'my_column', :uuid)
          cm1.should_not eql(cm2)
        end

        it 'is not equal to another column metadata when the column names differ' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'my_table', 'another_column', :uuid)
          cm1.should_not eql(cm2)
        end

        it 'is not equal to another column metadata when the types differ' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :timestamp)
          cm1.should_not eql(cm2)
        end
      end

      describe '#hash' do
        it 'is the same to another column metadata with the same properties' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm1.hash.should == cm2.hash
        end

        it 'is not the same when the keyspace names differ' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('another_keyspace', 'my_table', 'my_column', :uuid)
          cm1.hash.should_not == cm2.hash
        end

        it 'is not the same when the table names differ' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'another_table', 'my_column', :uuid)
          cm1.hash.should_not == cm2.hash
        end

        it 'is not the same when the column names differ' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'my_table', 'another_column', :uuid)
          cm1.hash.should_not == cm2.hash
        end

        it 'is not the same when the types differ' do
          cm1 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :uuid)
          cm2 = ColumnMetadata.new('my_keyspace', 'my_table', 'my_column', :timestamp)
          cm1.hash.should_not == cm2.hash
        end
      end
    end
  end
end
