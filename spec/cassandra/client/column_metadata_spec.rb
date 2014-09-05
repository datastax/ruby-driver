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
