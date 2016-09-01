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

module Cassandra
  class Cluster
    class Schema
      module Fetchers
        [
            ['1.2.19', V1_2_x, FQCNTypeParser],
            ['2.0.16', V2_0_x, FQCNTypeParser],
            ['2.1.9',  V2_1_x, FQCNTypeParser],
            ['2.2.1',  V2_2_x, FQCNTypeParser],
            ['3.0.0', V3_0_x, CQLTypeParser]
        ].each do |(version, klass, parser_class)|

          describe(klass) do
            let(:data) { JSON.load(File.read(File.dirname(__FILE__) + '/fetchers/' + version + '-data.json')) }
            let(:connection) { double('cql protocol handler') }
            let(:schema_type_parser) {
              parser_class.new
            }
            let(:cluster_schema) { double('schema') }
            subject { klass.new(schema_type_parser, cluster_schema) }

            before do
              allow(cluster_schema).to receive(:keyspace).and_return(nil)
            end

            describe('#fetch') do
              before do
                allow(connection).to receive(:send_request) do |r|
                  case r
                    when Protocol::QueryRequest
                      if data.include?(r.cql)
                        Ione::Future.resolved(Protocol::RowsResultResponse.new(nil, nil, data[r.cql], nil, nil, nil))
                      else
                        raise "unsupported cql: #{r.cql}"
                      end
                    else
                      raise "unexpected request: #{r.inspect}"
                  end
                end
              end

              it 'correctly reconstructs the entire schema' do
                parts = []
                keyspaces = subject.fetch(connection).value
                keyspaces.each do |keyspace|
                  parts << keyspace.to_cql
                  keyspace.each_table do |table|
                    parts << table.to_cql

                    table.each_index do |index|
                      parts << index.to_cql
                    end

                    table.each_trigger do |trigger|
                      parts << trigger.to_cql
                    end
                  end
                  keyspace.each_materialized_view do |view|
                    parts << view.to_cql
                  end
                end
                cql = parts.join("\n\n")
                cql += "\n"
                expect(cql).to eq(File.read(File.dirname(__FILE__) + '/fetchers/' + version + '-schema.cql'))
              end

              if klass == V3_0_x
                it 'reports table extensions properly' do
                  keyspaces = subject.fetch(connection).value
                  table = keyspaces.first.table('t1')
                  expect(table.options.extensions).to eq({ 'object_type' => 'ext table' })
                end

                it 'reports view extensions properly' do
                  keyspaces = subject.fetch(connection).value
                  view = keyspaces.first.materialized_view('v1')
                  expect(view.options.extensions).to eq({ 'object_type' => 'ext view' })
                end
              end
            end
          end
        end
      end
    end
  end
end
