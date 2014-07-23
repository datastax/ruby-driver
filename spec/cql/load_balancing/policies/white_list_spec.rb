# encoding: utf-8

require 'spec_helper'

module Cql
  module LoadBalancing
    module Policies
      describe(WhiteList) do
        let(:ips)            { ['127.0.0.1', '127.0.0.2'] }
        let(:wrapped_policy) { double('wrapped policy', :plan => nil) }

        let(:policy) { WhiteList.new(ips, wrapped_policy) }

        [:host_up, :host_down, :host_found, :host_lost].each do |method|
          describe("##{method}") do
            let(:host) { double('host') }

            it 'forwards to wrapped policy' do
              expect(wrapped_policy).to receive(method).once.with(host)
              policy.__send__(method, host)
            end
          end
        end

        describe('#distance') do
          context('host is whitelisted') do
            let(:host)     { Host.new(IPAddr.new(ips.first)) }
            let(:distance) { double('distance') }

            it 'forwards to wrapped policy' do
              expect(wrapped_policy).to receive(:distance).once.with(host).and_return(distance)
              expect(policy.distance(host)).to eq(distance)
            end
          end

          context('host is not whitelisted') do
            let(:host)     { Host.new(IPAddr.new('127.0.0.3')) }
            let(:distance) { policy.distance(host) }

            it 'is ignored' do
              expect(distance).to be_ignore
            end
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

          describe(WhiteList::Plan) do
            let(:wrapped_plan) { double('plan returned by wrapped policy') }

            let(:plan) { policy.plan(keyspace, statement, options) }

            before do
              wrapped_policy.stub(:plan).and_return(wrapped_plan)
            end

            describe('#next') do
              context('host is whitelisted') do
                let(:host) { Host.new(IPAddr.new(ips.first)) }

                before do
                  wrapped_plan.stub(:next).and_return(host)
                end

                it 'is returned' do
                  expect(plan.next).to eq(host)
                end
              end

              context('host is not whitelisted') do
                let(:host) { Host.new(IPAddr.new('127.0.0.3')) }
                let(:next_host) { Host.new(IPAddr.new(ips.first)) }

                before do
                  wrapped_plan.stub(:next).and_return(host, next_host)
                end

                it 'is skipped' do
                  expect(plan.next).to eq(next_host)
                end
              end

              context('exhausted') do
                before do
                  wrapped_plan.stub(:next).and_raise(::StopIteration)
                end

                it 'stops iteration' do
                  expect { plan.next }.to raise_error(::StopIteration)
                end
              end
            end
          end
        end
      end
    end
  end
end
