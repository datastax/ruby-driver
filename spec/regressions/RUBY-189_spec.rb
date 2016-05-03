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
    describe(Client) do
      let(:hosts) { ['127.0.0.1', '127.0.0.2'] }
      let(:io_reactor) { FakeIoReactor.new }
      let(:reconnection_policy) { Reconnection::Policies::Exponential.new(0.5, 30, 2) }
      let(:load_balancing_policy) { FakeLoadBalancingPolicy.new(cluster_registry) }
      let(:cluster_registry) { FakeClusterRegistry.new(hosts) }
      let(:logger) { Cassandra::NullLogger.new }
      let(:driver_settings) { {
          :io_reactor => io_reactor,
          :load_balancing_policy => load_balancing_policy,
          :cluster_registry => cluster_registry,
          :connections_per_local_node => 2,
          :connections_per_remote_node => 1,
          :reconnection_policy => reconnection_policy,
          :executor => Executors::SameThread.new,
          :logger => logger,
          :protocol_version => 2
      } }

      let(:driver) { Driver.new(driver_settings) }
      let(:client) {
        Client.new(driver.logger, driver.cluster_registry, driver.cluster_schema, driver.io_reactor, driver.connector,
                   driver.load_balancing_policy, driver.reconnection_policy, driver.retry_policy,
                   driver.address_resolution_policy, driver.connection_options, driver.futures_factory)
      }

      let(:promise) { double('promise') }
      let(:plan) { double('plan') }
      let(:connection) { double('connection') }
      let(:options) { {} }
      let(:errors) { {} }
      let(:hosts) { [] }
      let(:statement) { double('statement') }
      let(:request) { double('request') }
      let(:batch_statement) { double('batch_statement') }
      let(:bound_statement) { double('bound_statement') }

      it 'RUBY-189 - handles node down after prepare' do
        expect(promise).to_not receive(:break)
        expect(statement).to receive(:cql).and_return('select * from foo')
        expect(client).to receive(:execute_by_plan).with(promise,
                                                         'keyspace',
                                                         statement,
                                                         options,
                                                         'request',
                                                         plan,
                                                         12,
                                                         errors,
                                                         hosts)
        client.send(:prepare_and_send_request_by_plan,
                    'down_host',
                    connection,
                    promise,
                    'keyspace',
                    statement,
                    options,
                    'request',
                    plan,
                    12,
                    errors,
                    hosts)
      end

      it 'RUBY-189 - handles node down after prepare in batch' do
        expect(request).to receive(:clear)
        expect(bound_statement).to receive(:is_a?).and_return(true)
        expect(bound_statement).to receive(:cql).and_return('select * from foo')
        expect(batch_statement).to receive(:statements).and_return([bound_statement])
        expect(promise).to_not receive(:break)
        expect(client).to receive(:batch_by_plan).with(promise,
                                                       'keyspace',
                                                       batch_statement,
                                                       options,
                                                       request,
                                                       plan,
                                                       12,
                                                       errors,
                                                       hosts)
        client.send(:batch_and_send_request_by_plan,
                    'down_host',
                    connection,
                    promise,
                    'keyspace',
                    batch_statement,
                    request,
                    options,
                    plan,
                    12,
                    errors,
                    hosts)
      end
    end
  end
end
