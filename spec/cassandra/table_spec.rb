# encoding: utf-8

#--
# Copyright DataStax, Inc.
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

  describe(Table) do
    let(:ks) { double('keyspace') }
    let(:col) { double('col') }
    let(:col2) { double('col2') }
    let(:col3) { double('from') }
    let(:quote_column) { double('"my"col"') }
    let(:options) { double('options') }
    let(:id) { 1234 }
    before do
      allow(ks).to receive(:name).and_return('myks1')
      allow(col).to receive(:name).and_return('col')
      allow(col).to receive(:type).and_return(Cassandra::Types.int)
      allow(col2).to receive(:name).and_return('col2')
      allow(col2).to receive(:type).and_return(int)
      allow(col3).to receive(:name).and_return('from')
      allow(col3).to receive(:type).and_return(varchar)
      allow(quote_column).to receive(:name).and_return('"my"col"')
      allow(quote_column).to receive(:type).and_return(varchar)
      allow(options).to receive(:to_cql).and_return('opt1=value1')
    end

    context :index do
      let(:index1) { double('index1') }
      let(:index2) { double('index2') }
      let(:table) { Table.new(ks, 'mytable1', [col], [], [col2, col3, quote_column], options, [], id) }

      before do
        allow(index1).to receive(:name).and_return('index1')
        allow(index2).to receive(:name).and_return('index2')
        table.add_index(index1)
        table.add_index(index2)
      end

      context :has_index? do
        it 'should find an index that exists' do
          expect(table.has_index?('index1')).to be_truthy
        end

        it 'should fail to find an index that does not exist' do
          expect(table.has_index?('index3')).to be_falsey
        end
      end

      context :index do
        it 'should find an index that exists' do
          expect(table.index('index1')).to be(index1)
        end

        it 'should fail to find an index that does not exist' do
          expect(table.index('index3')).to be_nil
        end
      end

      it 'should return collection of indexes' do
        expect(table.indexes).to eq([index1, index2])
      end

      it 'should support iterating indexes' do
        result = []
        table.each_index do |index|
          result << index
        end

        expect(result).to eq([index1, index2])
      end
    end

    context :materialized_view do
      let(:view1) { double('view1') }
      let(:view2) { double('view2') }
      let(:table) { Table.new(ks, 'mytable1', [col], [], [col2, col3, quote_column], options, [], id) }

      before do
        allow(view1).to receive(:name).and_return('view1')
        allow(view2).to receive(:name).and_return('view2')
        table.add_view(view1)
        table.add_view(view2)
      end

      context :has_materialized_view? do
        it 'should find an materialized_view that exists' do
          expect(table.has_materialized_view?('view1')).to be_truthy
        end

        it 'should fail to find an materialized_view that does not exist' do
          expect(table.has_materialized_view?('view3')).to be_falsey
        end
      end

      context :materialized_view do
        it 'should find an materialized_view that exists' do
          expect(table.materialized_view('view1')).to be(view1)
        end

        it 'should fail to find an materialized_view that does not exist' do
          expect(table.materialized_view('view3')).to be_nil
        end
      end

      it 'should return collection of materialized_views' do
        expect(table.materialized_views).to eq([view1, view2])
      end

      it 'should support iterating materialized_views' do
        result = []
        table.each_materialized_view do |materialized_view|
          result << materialized_view
        end

        expect(result).to eq([view1, view2])
      end
    end

    context :trigger do
      let(:trigger1) { double('trigger1') }
      let(:trigger2) { double('trigger2') }
      let(:table) { Table.new(ks, 'mytable1', [col], [], [col2, col3, quote_column], options, [], id) }

      before do
        allow(trigger1).to receive(:name).and_return('trigger1')
        allow(trigger2).to receive(:name).and_return('trigger2')
        table.add_trigger(trigger1)
        table.add_trigger(trigger2)
      end

      context :has_trigger? do
        it 'should find an trigger that exists' do
          expect(table.has_trigger?('trigger1')).to be_truthy
        end

        it 'should fail to find an trigger that does not exist' do
          expect(table.has_trigger?('trigger3')).to be_falsey
        end
      end

      context :trigger do
        it 'should find an trigger that exists' do
          expect(table.trigger('trigger1')).to be(trigger1)
        end

        it 'should fail to find an trigger that does not exist' do
          expect(table.trigger('trigger3')).to be_nil
        end
      end

      it 'should return collection of triggers' do
        expect(table.triggers).to eq([trigger1, trigger2])
      end

      it 'should support iterating triggers' do
        result = []
        table.each_trigger do |trigger|
          result << trigger
        end

        expect(result).to eq([trigger1, trigger2])
      end
    end

    context :to_cql do

      it 'should quote keyspace, table, columns properly' do
        t = Table.new(ks, 'mytable1', [col], [], [col2, col3, quote_column], options, [], id)
        expected_cql = <<-EOF
CREATE TABLE "myks1"."mytable1" (
  col int PRIMARY KEY,
  "col2" int,
  "from" text,
  """my""col""" text
)
WITH opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end

      it 'should quote primary key properly for simple partition key' do
        t = Table.new(ks, 'mytable1', [col], [col2], [col3], options, [], id)
        expected_cql = <<-EOF
CREATE TABLE "myks1"."mytable1" (
  col int,
  "col2" int,
  "from" text,
  PRIMARY KEY (col, "col2")
)
WITH opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end

      it 'should quote primary key properly for composite partition key' do
        t = Table.new(ks, 'mytable1', [col, col2], [col3], [], options, [], id)
        expected_cql = <<-EOF
CREATE TABLE "myks1"."mytable1" (
  col int,
  "col2" int,
  "from" text,
  PRIMARY KEY ((col, "col2"), "from")
)
WITH opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end

      it 'should handle clustering order properly' do
        t = Table.new(ks, 'mytable1', [col, col2], [col3], [], options, [:asc], id)
        expected_cql = <<-EOF
CREATE TABLE "myks1"."mytable1" (
  col int,
  "col2" int,
  "from" text,
  PRIMARY KEY ((col, "col2"), "from")
)
WITH CLUSTERING ORDER BY ("from" ASC)
 AND opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end
    end
  end
end
