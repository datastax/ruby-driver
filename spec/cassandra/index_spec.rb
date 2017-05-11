# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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
  describe(Index) do
    let(:table) { double('table') }
    it 'should unescape target' do
      ind = Cassandra::Index.new(table, 'myind', :composites, 'f1', {})
      expect(ind.target).to eq('f1')

      ind = Cassandra::Index.new(table, 'myind', :composites, '"f1"', {})
      expect(ind.target).to eq('f1')
    end

    context :to_cql do
      let(:ks) { double('keyspace') }
      let(:col) { double('col') }
      let(:col2) { double('col2') }
      let(:col3) { double('from') }
      let(:options) { {'target' => 'f1'} }
      let(:id) { 1234 }
      let(:where) { 'col=7' }

      before do
        allow(ks).to receive(:name).and_return('myks1')
        allow(table).to receive(:name).and_return('table1')
        allow(table).to receive(:keyspace).and_return(ks)
      end

      it 'should quote keyspace, view name, table name, columns properly for regular index' do
        t = Cassandra::Index.new(table, 'myind1', :composites, 'f1', options)
        expect(t.to_cql).to eq('CREATE INDEX "myind1" ON "myks1"."table1" ("f1");')
      end

      it 'should quote keyspace, view name, table name, columns properly for custom index' do
        options['opt1'] = 'val1'
        options['class_name'] = 'com.datastax.Custom'
        t = Cassandra::Index.new(table, 'myind1', :custom, 'f1', options)
        expected = <<-EOF
CREATE CUSTOM INDEX "myind1" ON "myks1"."table1" ("f1") USING 'com.datastax.Custom' WITH OPTIONS = {'opt1': 'val1'};
EOF
        expect(t.to_cql).to eq(expected.chomp)
      end

      it 'should exclude "WITH OPTIONS" if there are no options' do
        options['class_name'] = 'com.datastax.Custom'
        t = Cassandra::Index.new(table, 'myind1', :custom, 'f1', options)
        expected = <<-EOF
CREATE CUSTOM INDEX "myind1" ON "myks1"."table1" ("f1") USING 'com.datastax.Custom';
        EOF
        expect(t.to_cql).to eq(expected.chomp)
      end
    end
  end
end
