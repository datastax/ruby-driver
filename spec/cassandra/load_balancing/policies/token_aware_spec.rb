# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
      describe(TokenAware) do
        let(:policy)  { double('load balancing policy') }
        let(:cluster) { double('cassandra cluster') }

        subject { TokenAware.new(policy) }

        before do
          allow(policy).to receive(:respond_to?) { true }
        end

        describe('#setup') do
          it 'sets up wrapped policy' do
            expect(policy).to receive(:setup).once.with(cluster)
            subject.setup(cluster)
          end
        end

        [
          :distance,
          :host_found,
          :host_up,
          :host_down,
          :host_lost
        ].each do |method|
          describe("##{method}") do
            let(:host) { double('cassandra host') }

            it 'delegates to wrapped policy' do
              expect(policy).to receive(method).once.with(host)
              subject.send(method, host)
            end
          end
        end

        describe('#plan') do
          let(:keyspace)  { 'keyspace' }
          let(:statement) { double('statement') }
          let(:options)   { double('execution options') }

          context('when not set up') do
            let(:plan) { double('wrapped policy plan') }

            it 'delegates to wrapped policy' do
              expect(policy).to receive(:plan).once.with(keyspace, statement, options).and_return(plan)
              expect(subject.plan(keyspace, statement, options)).to eq(plan)
            end
          end

          context('when set up') do
            before do
              allow(policy).to receive(:setup)
              subject.setup(cluster)
              expect(cluster).to receive(:find_replicas).once.with(keyspace, statement).and_return(replicas)
            end

            context('and replicas not found') do
              let(:replicas) { [] }
              let(:plan)     { double('wrapped policy plan') }

              it 'delegates to wrapped policy' do
                expect(policy).to receive(:plan).once.with(keyspace, statement, options).and_return(plan)
                expect(subject.plan(keyspace, statement, options)).to eq(plan)
              end
            end

            context('and replicas found') do
              let(:replicas) {
                [
                  Cassandra::Host.new('127.0.0.1'),
                  Cassandra::Host.new('127.0.0.2'),
                  Cassandra::Host.new('127.0.0.3'),
                ]
              }
              let(:plan) { subject.plan(keyspace, statement, options) }

              context('and all replicas are local') do
                before do
                  allow(replicas).to receive(:shuffle) { replicas.dup }
                  allow(policy).to receive(:distance) { :local }
                end

                it 'prioritizes found replicas' do
                  expect(plan.has_next?).to eq(true)
                  expect(plan.next).to eq(replicas[0])
                  expect(plan.has_next?).to eq(true)
                  expect(plan.next).to eq(replicas[1])
                  expect(plan.has_next?).to eq(true)
                  expect(plan.next).to eq(replicas[2])
                end

                context('and all replicas failed') do
                  before do
                    replicas.size.times do
                      expect(plan.has_next?).to eq(true)
                      plan.next
                    end

                    allow(child_plan).to receive(:next).and_return(next_host)
                    allow(child_plan).to receive(:has_next?).and_return(true, false)
                    allow(policy).to receive(:plan).and_return(child_plan)
                  end

                  let(:next_host)  { double('next host from the wrapped policy plan') }
                  let(:child_plan) { double('wrapped policy plan') }

                  it 'delegates to the wrapped policy' do
                    expect(plan.has_next?).to be_truthy
                    expect(plan.next).to eq(next_host)
                  end

                  context('and replica host returned from the child plan') do
                    let(:next_host) { replicas.sample }

                    it 'is ignored' do
                      expect(plan.has_next?).to be_falsey
                    end
                  end
                end
              end

              context('and all replicas are not local') do
                before do
                  allow(policy).to receive(:distance) { :remote }
                  allow(policy).to receive(:plan)     { child_plan }

                  allow(child_plan).to receive(:next)      { next_host }
                  allow(child_plan).to receive(:has_next?) { true }
                end

                let(:next_host)  { double('next host from the wrapped policy plan') }
                let(:child_plan) { double('wrapped policy plan') }

                it 'delegates to the wrapped policy' do
                  expect(plan.has_next?).to be_truthy
                  expect(plan.next).to eq(next_host)
                end
              end
            end
          end
        end
      end
    end
  end
end
