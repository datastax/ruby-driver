# encoding: utf-8

require 'spec_helper'

module Cql
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
          let(:write_type)    { 'SIMPLE' }
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

          context('write_type=BATCH_LOG') do
            let(:write_type) { 'BATCH_LOG' }

            it 'retries with the same consistency' do
              expect(decision).to be_a(Decisions::Retry)
              expect(decision.consistency).to eq(consistency)
            end
          end

          context('other write_types') do
            let(:write_type) { 'OTHER' }

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
