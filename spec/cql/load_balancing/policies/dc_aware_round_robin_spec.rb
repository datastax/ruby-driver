# encoding: utf-8

require 'spec_helper'

module Cql
  module LoadBalancing
    module Policies
      describe(DCAwareRoundRobin) do
        let(:datacenter)                             { 'DC1' }
        let(:max_remote_hosts_to_use)                { nil }
        let(:use_remote_hosts_for_local_consistency) { false }

        let(:policy) { DCAwareRoundRobin.new(datacenter, max_remote_hosts_to_use, use_remote_hosts_for_local_consistency) }

        let(:host_datacenter) { datacenter }

        let(:host) { Host.new('127.0.0.1', nil, nil, host_datacenter) }

        let(:distance) { policy.distance(host) }

        describe('#host_up') do
          before do
            policy.host_up(host)
          end

          context('host is in a different datacenter') do
            let(:host_datacenter) { 'DC2' }

            context('remote hosts are ignored') do
              let(:max_remote_hosts_to_use) { 0 }

              it 'is ignored' do
                expect(distance).to be_ignore
              end
            end

            context('only one remote host can be used') do
              let(:max_remote_hosts_to_use) { 1 }

              it 'is remote' do
                expect(distance).to be_remote
              end

              context('another host in remote datacenter is up') do
                let(:another_host) { Host.new('127.0.0.2', nil, nil, host_datacenter) }

                before do
                  policy.host_up(another_host)
                end

                let(:another_distance) { policy.distance(another_host) }

                it 'is ignored' do
                  expect(another_distance).to be_ignore
                end
              end
            end
          end

          context('host is in the same datacenter') do
            let(:host_datacenter) { datacenter }

            it 'is local' do
              expect(distance).to be_local
            end
          end

          context("host's datacenter is unknown") do
            let(:host_datacenter) { nil }

            it 'is local' do
              expect(distance).to be_local
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
              expect(distance).to be_ignore
            end
          end

          context('host is in a different datacenter') do
            let(:host_datacenter) { 'DC2' }

            it 'starts being ignored' do
              expect(distance).to be_ignore
            end
          end
        end

        describe('#distance') do
          context('host is in a different datacenter') do
            let(:host_datacenter) { 'DC2' }

            it 'ignores unknown hosts' do
              expect(policy.distance(host)).to be_ignore
            end
          end

          context('host is in the same datacenter') do
            let(:host_datacenter) { datacenter }

            it 'ignores unknown hosts' do
              expect(policy.distance(host)).to be_ignore
            end
          end

          context("host's datacenter is unknown") do
            let(:host_datacenter) { nil }

            it 'ignores unknown hosts' do
              expect(policy.distance(host)).to be_ignore
            end
          end
        end

        describe('#plan') do
          let(:keyspace)    { 'foo' }
          let(:statement)   { VOID_STATEMENT }
          let(:consistency) { :one }
          let(:options)     { Execution::Options.new({:consistency => consistency}) }

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
          end

          it 'prioritizes hosts first' do
            5.times do
              host = plan.next
              expect(policy.distance(host)).to be_local
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
                    expect { plan.next }.to raise_error(::StopIteration)
                  end
                end

                context('consistency is not local') do
                  let(:consistency) { :one }

                  it 'returns remote hosts last' do
                    5.times do
                      host = plan.next
                      expect(policy.distance(host)).to be_remote
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
                      expect(policy.distance(host)).to be_remote
                    end
                  end
                end

                context('consistency is not local') do
                  let(:consistency) { :one }

                  it 'returns remote hosts' do
                    5.times do
                      host = plan.next
                      expect(policy.distance(host)).to be_remote
                    end
                  end
                end
              end

              context('remote hosts exhausted') do
                before do
                  5.times { plan.next }
                end

                it 'stops iteration' do
                  expect { plan.next }.to raise_error(::StopIteration)
                end
              end
            end

            context('remote hosts cannot be used') do
              let(:max_remote_hosts_to_use) { 0 }

              it 'stops iteration' do
                expect { plan.next }.to raise_error(::StopIteration)
              end
            end

            context('limited number of remote hosts can be used') do
              let(:max_remote_hosts_to_use) { 3 }

              it 'returns up to a given number of remote hosts' do
                max_remote_hosts_to_use.times do
                  plan.next
                end

                expect { plan.next }.to raise_error(::StopIteration)
              end
            end
          end
        end
      end
    end
  end
end
