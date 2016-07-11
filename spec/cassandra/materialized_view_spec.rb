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

  describe(MaterializedView) do
    context :to_cql do
      let(:ks) { double('keyspace')}
      let(:table) { double('table')}
      let(:col) { double('col')}
      let(:col2) { double('col2')}
      let(:col3) { double('from')}
      let(:options) { double('options')}
      let(:id) { 1234 }
      let(:where) { 'col=7'}

      before do
        allow(ks).to receive(:name).and_return('myks1')
        allow(ks).to receive(:table).with('table1').and_return(table)
        allow(table).to receive(:name).and_return('table1')
        allow(col).to receive(:name).and_return('col')
        allow(col).to receive(:type).and_return(Cassandra::Types.int)
        allow(col2).to receive(:name).and_return('col2')
        allow(col2).to receive(:type).and_return(int)
        allow(col3).to receive(:name).and_return('from')
        allow(col3).to receive(:type).and_return(varchar)
        allow(options).to receive(:to_cql).and_return('opt1=value1')
      end

      it 'should quote keyspace, view name, table name, columns properly' do
        t = MaterializedView.new(ks, 'myview1', [col], [], [col2, col3], options, false, where, 'table1', id)
        expected_cql = <<-EOF
CREATE MATERIALIZED VIEW "myks1"."myview1" AS
SELECT col, "col2", "from"
FROM "myks1"."table1"
WHERE col=7
PRIMARY KEY ((col))
WITH opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end

      it 'should quote primary key properly for simple partition key' do
        t = MaterializedView.new(ks, 'myview1', [col], [col2], [col3], options, false, where, 'table1', id)
        expected_cql = <<-EOF
CREATE MATERIALIZED VIEW "myks1"."myview1" AS
SELECT col, "col2", "from"
FROM "myks1"."table1"
WHERE col=7
PRIMARY KEY ((col), "col2")
WITH opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end

      it 'should quote primary key properly for composite partition key' do
        t = MaterializedView.new(ks, 'myview1', [col, col2], [col3], [], options, false, where, 'table1', id)
        expected_cql = <<-EOF
CREATE MATERIALIZED VIEW "myks1"."myview1" AS
SELECT col, "col2", "from"
FROM "myks1"."table1"
WHERE col=7
PRIMARY KEY ((col, "col2"), "from")
WITH opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end

      it 'should handle no where-clause properly' do
        t = MaterializedView.new(ks, 'myview1', [col, col2], [col3], [], options, false, nil, 'table1', id)
        expected_cql = <<-EOF
CREATE MATERIALIZED VIEW "myks1"."myview1" AS
SELECT col, "col2", "from"
FROM "myks1"."table1"
PRIMARY KEY ((col, "col2"), "from")
WITH opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end

      it 'should handle include-all-columns properly' do
        t = MaterializedView.new(ks, 'myview1', [col, col2], [col3], [], options, true, nil, 'table1', id)
        expected_cql = <<-EOF
CREATE MATERIALIZED VIEW "myks1"."myview1" AS
SELECT *
FROM "myks1"."table1"
PRIMARY KEY ((col, "col2"), "from")
WITH opt1=value1;
        EOF
        expect(t.to_cql).to eq(expected_cql.chomp)
      end
    end
  end
end
