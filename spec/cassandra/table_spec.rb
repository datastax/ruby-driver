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

  describe(Table) do
    context :to_cql do
      let(:ks) { double('keyspace')}
      let(:col) { double('col')}
      let(:col2) { double('col2')}
      let(:col3) { double('from')}
      let(:options) { double('options')}
      let(:id) { 1234 }
      before do
        allow(ks).to receive(:name).and_return('myks1')
        allow(col).to receive(:name).and_return('col')
        allow(col).to receive(:type).and_return(Cassandra::Types.int)
        allow(col2).to receive(:name).and_return('col2')
        allow(col2).to receive(:type).and_return(int)
        allow(col3).to receive(:name).and_return('from')
        allow(col3).to receive(:type).and_return(varchar)
        allow(options).to receive(:to_cql).and_return('opt1=value1')
      end

      it 'should quote keyspace, table, columns properly' do
        t = Table.new(ks, 'mytable1', [col], [], [col2, col3], options, [], id)
        expected_cql = <<-EOF
CREATE TABLE "myks1"."mytable1" (
  col int PRIMARY KEY,
  "col2" int,
  "from" text
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
        t = Table.new(ks, 'mytable1', [col, col2], [col3], [], options, [:desc], id)
        expected_cql = <<-EOF
CREATE TABLE "myks1"."mytable1" (
  col int,
  "col2" int,
  "from" text,
  PRIMARY KEY ((col, "col2"), "from")
)
WITH CLUSTERING ORDER BY ("from" DESC)
 AND opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end
    end
  end
end
