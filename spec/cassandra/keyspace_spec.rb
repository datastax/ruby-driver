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

require 'spec_helper'

include Cassandra::Types
module Cassandra
  describe(Keyspace) do
    let(:view) { double('view') }
    let(:view2) { double('view2') }
    let(:table) { double('table') }
    let(:table2) { double('table2') }
    let(:index1) { double('index') }
    let(:index2) { double('index2') }

    context :view do
      let(:ks) { Keyspace.new('myks', true, nil, { 'mytable' => table, 'tbl2' => table2 },
                              nil, nil, nil, { 'myview' => view, 'view2' => view2 })
      }

      before do
        allow(view).to receive(:set_keyspace)
        allow(view2).to receive(:set_keyspace)
        allow(view2).to receive(:base_table)
        allow(table).to receive(:set_keyspace)
        allow(table2).to receive(:set_keyspace)
        allow(table).to receive(:each_index)
        allow(table2).to receive(:each_index)
      end

      context :has_materialized_view? do
        it 'should return true if the view exists and has a base-table' do
          expect(view).to receive(:base_table).and_return(table).exactly(2).times
          expect(table).to receive(:add_view).with(view)
          expect(ks.has_materialized_view?('myview')).to be_truthy
        end

        it 'should return false if the view exists but does not have a base-table' do
          expect(view).to receive(:base_table).and_return(nil).exactly(2).times
          expect(ks.has_materialized_view?('myview')).to be_falsey
        end
      end

      context :materialized_view do
        it 'should return the view if it exists and has a base-table' do
          expect(view).to receive(:base_table).and_return(table).exactly(2).times
          expect(table).to receive(:add_view).with(view)
          expect(ks.materialized_view('myview')).to be(view)
        end

        it 'should return nil if the view exists but does not have a base-table' do
          expect(view).to receive(:base_table).and_return(nil).exactly(2).times
          expect(ks.materialized_view('myview')).to be_nil
        end
      end

      context :materialized_views do
        it 'should return the view if it exists and has a base-table' do
          expect(view).to receive(:base_table).and_return(table).exactly(2).times
          expect(table).to receive(:add_view).with(view)
          expect(ks.materialized_views).to eq([view])
        end

        it 'should not return view if the view exists but does not have a base-table' do
          expect(view).to receive(:base_table).and_return(nil).exactly(2).times
          expect(ks.materialized_views).to be_empty
        end
      end

      context :each_materialized_view do
        it 'should return the view if it exists and has a base-table' do
          expect(view).to receive(:base_table).and_return(table).exactly(2).times
          expect(table).to receive(:add_view).with(view)
          result = []
          ks.each_materialized_view do |v|
            result << v
          end

          expect(result).to eq([view])
        end

        it 'should not return view if the view exists but does not have a base-table' do
          expect(view).to receive(:base_table).and_return(nil).exactly(2).times
          result = []
          ks.each_materialized_view do |v|
            result << v
          end

          expect(result).to be_empty
        end
      end
    end

    context :index do
      let(:ks) { Keyspace.new('myks', true, nil, { 'mytable' => table, 'tbl2' => table2 }, nil, nil, nil, {}) }

      before do
        allow(table).to receive(:set_keyspace)
        allow(table2).to receive(:set_keyspace)
        allow(table2).to receive(:each_index) do |&block|
          block.call(index1)
          block.call(index2)
        end
        allow(table).to receive(:each_index)
        allow(index1).to receive(:name).and_return("index1")
        allow(index2).to receive(:name).and_return("index2")
      end

      context :has_index? do
        it 'should return true if the index exists' do
          expect(ks.has_index?('index1')).to be_truthy
        end

        it 'should return false if the index does not exist' do
          expect(ks.has_index?('myindex')).to be_falsey
        end
      end

      context :index do
        it 'should return the index if it exists' do
          expect(ks.index('index1')).to be(index1)
        end

        it 'should return nil if the index does not exist' do
          expect(ks.index('myindex')).to be_nil
        end
      end

      context :indexes do
        it 'should return the indexes' do
          expect(ks.indexes).to eq([index1, index2])
        end
      end

      context :each_index do
        it 'should iterate the indexes' do
          result = []
          ks.each_index do |v|
            result << v
          end

          expect(result).to eq([index1, index2])
        end
      end
    end
  end
end
