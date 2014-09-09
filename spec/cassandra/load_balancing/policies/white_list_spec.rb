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
  module LoadBalancing
    module Policies
      describe(WhiteList) do
        let(:ips)            { ['127.0.0.1', '127.0.0.2'] }
        let(:wrapped_policy) { double('wrapped policy', :plan => nil) }

        let(:policy) { WhiteList.new(ips, wrapped_policy) }

        before do
          wrapped_policy.stub(:respond_to?) {|method| true}
        end

        [:host_up, :host_down, :host_found, :host_lost].each do |method|
          describe("##{method}") do
            context('host is whitelisted') do
              let(:host) { Host.new(IPAddr.new(ips.first)) }

              it 'forwards to wrapped policy' do
                expect(wrapped_policy).to receive(method).once.with(host)
                policy.__send__(method, host)
              end
            end

            context('host is not whitelisted') do
              let(:host) { Host.new(IPAddr.new('127.0.0.3')) }

              it 'does not forward to wrapped policy' do
                expect(wrapped_policy).to_not receive(method)
                policy.__send__(method, host)
              end
            end
          end
        end

        describe('#distance') do
          let(:host) { double('Host') }

          it 'forwards to wrapped policy' do
            expect(wrapped_policy).to receive(:distance).once.with(host)
            policy.distance(host)
          end
        end

        describe('#plan') do
          let(:keyspace)  { 'foo' }
          let(:statement) { VOID_STATEMENT }
          let(:options)   { VOID_OPTIONS }

          it 'forwards to wrapped policy' do
            expect(wrapped_policy).to receive(:plan).once.with(keyspace, statement, options)
            policy.plan(keyspace, statement, options)
          end
        end
      end
    end
  end
end
