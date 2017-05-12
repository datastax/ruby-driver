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
  module LoadBalancing
    module Policies
      describe(RoundRobin) do
        let(:policy) { RoundRobin.new }

        describe('#distance') do
          let(:host) { double('host') }

          context('with unknown host') do
            it 'is ignore' do
              expect(policy.distance(host)).to eq(:ignore)
            end
          end

          context('with known host') do
            before do
              policy.host_up(host)
            end

            it 'is local' do
              expect(policy.distance(host)).to eq(:local)
            end
          end
        end

        describe('#host_up') do
          let(:host) { double('host') }
          let(:keyspace)  { 'system' }
          let(:statement) { double('statement') }
          let(:options)   { double('execution options') }

          it 'adds the host to the rotation' do
            policy.host_up(host)

            plan = policy.plan(keyspace, statement, options)
            expect(plan).to have_next
            expect(plan.next).to eq(host)
          end
        end

        describe('#host_down') do
          let(:host) { double('host') }
          let(:keyspace)  { 'system' }
          let(:statement) { double('statement') }
          let(:options)   { double('execution options') }

          it 'removes the host from the rotation' do
            policy.host_up(host)
            policy.host_down(host)

            plan = policy.plan(keyspace, statement, options)
            expect(plan).to_not have_next
          end
        end

        describe('#plan') do
          let(:keyspace)  { 'system' }
          let(:statement) { double('statement') }
          let(:options)   { double('execution options') }

          context('when no hosts are up') do
            it 'returns an empty plan' do
              plan = policy.plan(keyspace, statement, options)
              expect(plan).to_not have_next
            end
          end

          context('when a host is up') do
            let(:host) { double('host') }

            it 'returns a plan with that host' do
              policy.host_up(host)

              plan = policy.plan(keyspace, statement, options)
              expect(plan).to have_next
              expect(plan.next).to eq(host)
            end
          end

          context('when several hosts are up') do
            let(:hosts) { Array.new(4) {|i| double("host[#{i}]")} }

            it 'returns a plan that round robins among hosts' do
              hosts.each do |host|
                policy.host_up(host)
              end

              hosts.size.times do |i|
                plan = policy.plan(keyspace, statement, options)

                hosts.rotate(i).each do |h|
                  expect(plan.next).to eq(h)
                end
              end
            end
          end
        end
      end
    end
  end
end
