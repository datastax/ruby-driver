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
      describe(DCAwareRoundRobin) do
        let(:datacenter)                             { 'DC1' }
        let(:max_remote_hosts_to_use)                { 0 }
        let(:use_remote_hosts_for_local_consistency) { false }

        let(:policy) { DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use, use_remote_hosts_for_local_consistency) }

        let(:host_datacenter) { datacenter }

        let(:host) { Host.new('127.0.0.1', nil, nil, host_datacenter) }

        let(:distance) { policy.distance(host) }

        describe '#constructor' do
          let(:use_remote_hosts_for_local_consistency) { true }
          it 'should error out if use_remmote_hosts_for_local_consistency is true and max_remote_hosts_to_use is 0' do
            expect {
              policy
            }.to raise_error(ArgumentError)
          end
        end

        describe('#host_up') do
          before do
            policy.host_up(host)
          end

          context('when no datacenter provided in constructor') do
            let(:datacenter)      { nil }
            let(:host_datacenter) { 'DC1' }
            let(:distance)        { policy.distance(host) }

            it 'is local' do
              expect(distance).to eq(:local)
            end

            context('and another host in remote datacenter is up') do
              let(:max_remote_hosts_to_use)                { nil }
              let(:another_host) { Host.new('127.0.0.2', nil, nil, host_datacenter) }

              before do
                policy.host_up(another_host)
              end

              let(:another_distance) { policy.distance(another_host) }

              it 'is also local' do
                expect(another_distance).to eq(:local)
              end

              context('and a host in a different datacenter is up') do
                let(:third_host)     { Host.new('127.0.0.3', nil, nil, 'DC2') }
                let(:third_distance) { policy.distance(third_host) }

                before do
                  policy.host_up(third_host)
                end

                it 'is remote' do
                  expect(third_distance).to eq(:remote)
                end
              end
            end
          end

          context('host is in a different datacenter') do
            let(:host_datacenter) { 'DC2' }

            context('remote hosts are ignored') do
              let(:max_remote_hosts_to_use) { 0 }

              it 'is ignored' do
                expect(distance).to eq(:ignore)
              end
            end

            context('only one remote host can be used') do
              let(:max_remote_hosts_to_use) { 1 }

              it 'is remote' do
                expect(distance).to eq(:remote)
              end

              context('another host in remote datacenter is up') do
                let(:another_host) { Host.new('127.0.0.2', nil, nil, host_datacenter) }

                before do
                  policy.host_up(another_host)
                end

                let(:another_distance) { policy.distance(another_host) }

                it 'is ignored' do
                  expect(another_distance).to eq(:ignore)
                end
              end
            end
          end

          context('host is in the same datacenter') do
            let(:host_datacenter) { datacenter }

            it 'is local' do
              expect(distance).to eq(:local)
            end
          end

          context("host's datacenter is unknown") do
            let(:host_datacenter) { nil }

            it 'is local' do
              expect(distance).to eq(:local)
            end
          end
        end

        describe('#host_down') do
          before do
            policy.host_up(host)
            policy.host_down(host)
          end

          context('host is in the same datacenter') do
            let(:host_datacenter) { datacenter }

            it 'starts being ignored' do
              expect(distance).to eq(:ignore)
            end
          end

          context('host is in a different datacenter') do
            let(:host_datacenter) { 'DC2' }

            it 'starts being ignored' do
              expect(distance).to eq(:ignore)
            end
          end
        end

        describe('#distance') do
          context('host is in a different datacenter') do
            let(:host_datacenter) { 'DC2' }

            it 'ignores unknown hosts' do
              expect(policy.distance(host)).to eq(:ignore)
            end
          end

          context('host is in the same datacenter') do
            let(:host_datacenter) { datacenter }

            it 'ignores unknown hosts' do
              expect(policy.distance(host)).to eq(:ignore)
            end
          end

          context("host's datacenter is unknown") do
            let(:host_datacenter) { nil }

            it 'ignores unknown hosts' do
              expect(policy.distance(host)).to eq(:ignore)
            end
          end
        end

        describe('#plan') do
          let(:keyspace)    { 'foo' }
          let(:statement)   { VOID_STATEMENT }
          let(:consistency) { :one }
          let(:retry_policy) { double('retry_policy') }
          let(:options)     {
            Execution::Options.new({ consistency: consistency,
                                     load_balancing_policy: policy,
                                     retry_policy: retry_policy})
          }

          let(:plan) { policy.plan(keyspace, statement, options) }

          let(:remote_datacenter) { 'DC2' }

          let(:remote_hosts) do
            5.times.map do |i|
              Host.new("127.0.0.#{i + 10}", nil, nil, remote_datacenter)
            end
          end

          let(:local_hosts) do
            4.times.map do |i|
              Host.new("127.0.0.#{i + 2}", nil, nil, datacenter)
            end
          end

          before do
            policy.host_up(host)
            remote_hosts.each {|host| policy.host_up(host)}
            local_hosts.each {|host| policy.host_up(host)}

            allow(retry_policy).to receive(:read_timeout)
            allow(retry_policy).to receive(:write_timeout)
            allow(retry_policy).to receive(:unavailable)
          end

          it 'prioritizes hosts first' do
            5.times do
              host = plan.next
              expect(policy.distance(host)).to eq(:local)
            end
          end

          context('local hosts exhausted') do
            before do
              5.times { plan.next }
            end

            context('remote hosts can be used') do
              let(:max_remote_hosts_to_use) { nil }

              context('remote hosts cannot be used for local consistencies') do
                let(:use_remote_hosts_for_local_consistency) { false }

                context('consistency is local') do
                  let(:consistency) { :local_quorum }

                  it 'stops iteration' do
                    expect(plan).to_not have_next
                  end
                end

                context('consistency is not local') do
                  let(:consistency) { :one }

                  it 'returns remote hosts last' do
                    5.times do
                      host = plan.next
                      expect(policy.distance(host)).to eq(:remote)
                    end
                  end
                end
              end

              context('remote hosts can be used for local consistencies') do
                let(:use_remote_hosts_for_local_consistency) { true }

                context('consistency is local') do
                  let(:consistency) { :local_quorum }

                  it 'returns remote hosts' do
                    5.times do
                      host = plan.next
                      expect(policy.distance(host)).to eq(:remote)
                    end
                  end
                end

                context('consistency is not local') do
                  let(:consistency) { :one }

                  it 'returns remote hosts' do
                    5.times do
                      host = plan.next
                      expect(policy.distance(host)).to eq(:remote)
                    end
                  end
                end
              end

              context('remote hosts exhausted') do
                before do
                  5.times { plan.next }
                end

                it 'stops iteration' do
                  expect(plan).to_not have_next
                end
              end
            end

            context('remote hosts cannot be used') do
              let(:max_remote_hosts_to_use) { 0 }

              it 'stops iteration' do
                expect(plan).to_not have_next
              end
            end

            context('limited number of remote hosts can be used') do
              let(:max_remote_hosts_to_use) { 3 }

              it 'returns up to a given number of remote hosts' do
                max_remote_hosts_to_use.times do
                  plan.next
                end

                expect(plan).to_not have_next
              end
            end
          end
        end
      end
    end
  end
end
