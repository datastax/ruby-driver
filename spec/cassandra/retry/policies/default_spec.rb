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
  module Retry
    module Policies
      describe(Default) do
        let(:policy) { Default.new }

        describe('#read_timeout') do
          let(:statement)          { VOID_STATEMENT }
          let(:consistency)        { :one }
          let(:required_responses) { 1 }
          let(:received_responses) { 0 }
          let(:data_retrieved)     { false }
          let(:retries)            { 0 }

          let(:decision) do
            policy.read_timeout(statement, consistency, required_responses,
                                received_responses, data_retrieved, retries)
          end

          context('second attempt') do
            let(:retries)            { 1 }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end

          context('not enough responses') do
            let(:required_responses) { 1 }
            let(:received_responses) { 0 }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end

          context('enough responses') do
            let(:required_responses) { 1 }
            let(:received_responses) { 1 }

            context('data not retrieved') do
              let(:data_retrieved) { false }

              it 'retries with the same consistency' do
                expect(decision).to be_a(Decisions::Retry)
                expect(decision.consistency).to eq(consistency)
              end
            end

            context('data retrieved') do
              let(:data_retrieved) { true }

              it 'reraises' do
                expect(decision).to be_a(Decisions::Reraise)
              end
            end
          end
        end

        describe('#write_timeout') do
          let(:statement)     { VOID_STATEMENT }
          let(:consistency)   { :one }
          let(:write_type)    { :simple }
          let(:acks_required) { 1 }
          let(:acks_received) { 0 }
          let(:retries)       { 0 }

          let(:decision) do
            policy.write_timeout(statement, consistency, write_type,
                                 acks_required, acks_received, retries)
          end

          context('second attempt') do
            let(:retries)            { 1 }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end

          context('write_type=:batch_log') do
            let(:write_type) { :batch_log }

            it 'retries with the same consistency' do
              expect(decision).to be_a(Decisions::Retry)
              expect(decision.consistency).to eq(consistency)
            end
          end

          context('other write_types') do
            let(:write_type) { :other }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end
        end

        describe('#unavailable') do
          let(:statement)         { VOID_STATEMENT }
          let(:consistency)       { :one }
          let(:replicas_required) { 1 }
          let(:replicas_alive)    { 0 }
          let(:data_retrieved)    { false }
          let(:retries)           { 0 }

          let(:decision) do
            policy.unavailable(statement, consistency, replicas_required,
                               replicas_alive, retries)
          end

          it 'reraises' do
            expect(decision).to be_a(Decisions::Reraise)
          end
        end
      end
    end
  end
end
