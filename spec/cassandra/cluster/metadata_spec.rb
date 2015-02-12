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

# @private
class FakeSchema < Cassandra::Cluster::Schema
  def initialize
    super(nil)
  end

  def add_keyspace(name, replication_class, replication_options)
    replication = Cassandra::Keyspace::Replication.new(replication_class, replication_options)
    @keyspaces[name] = Cassandra::Keyspace.new(name, true, replication, {}, {})
    self
  end
end

module Cassandra
  class Cluster
    describe(Metadata) do
      describe('#find_replicas') do
        let(:schema)   { FakeSchema.new }
        let(:driver)   { Driver.new(:cluster_schema => schema) }
        let(:registry) { driver.cluster_registry }

        subject { driver.cluster_metadata }

        context('with murmur3 partitioner') do
          let(:hosts) {
            {
              '127.0.0.1' => {
                'host_id'     => 1,
                'rack'        => 'rack1',
                'data_center' => 'dc1',
                'tokens'      => ['-9000000000000000000']
              },
              '127.0.0.2' => {
                'host_id'     => 2,
                'rack'        => 'rack1',
                'data_center' => 'dc1',
                'tokens'      => ['-3000000000000000000']
              },
              '127.0.0.3' => {
                'host_id'     => 3,
                'rack'        => 'rack1',
                'data_center' => 'dc1',
                'tokens'      => ['2000000000000000000']
              },
            }
          }

          let(:keyspace) { 'simplex' }

          before do
            schema.add_keyspace(keyspace, 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor' => 3)

            hosts.each do |address, data|
              registry.host_found(address, data)
            end

            subject.update({
              'name'        => 'cluster-name',
              'partitioner' => 'org.apache.cassandra.dht.Murmur3Partitioner'
            })
            subject.rebuild_token_map
          end

          it 'works' do
            statement = double('statement')
            allow(statement).to receive(:respond_to?).with(:keyspace).and_return(true)
            allow(statement).to receive(:respond_to?).with(:partition_key).and_return(true)
            allow(statement).to receive(:keyspace)
            allow(statement).to receive(:partition_key).and_return('qwe')
            subject.find_replicas(keyspace, statement)
          end
        end
      end
    end
  end
end
