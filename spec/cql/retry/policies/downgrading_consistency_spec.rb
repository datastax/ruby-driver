# encoding: utf-8

require 'spec_helper'

module Cql
  module Retry
    module Policies
      describe(DowngradingConsistency) do
        let(:policy) { DowngradingConsistency.new }

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
            let(:retries) { 1 }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end

          [:serial, :local_serial].each do |consistency|
            context("consistency=#{consistency.inspect}") do
              let(:consistency) { consistency }

              it 'reraises' do
                expect(decision).to be_a(Decisions::Reraise)
              end
            end
          end

          [
            [7, 4, :all, :quorum],
            [7, 3, :all, :three],
            [7, 2, :all, :two],
            [7, 1, :all, :one],
          ].each do |(required, received, consistency, retry_consistency)|
            context("required_responses=#{required} received_responses=#{received} consistency=#{consistency.inspect}") do
              let(:consistency)        { consistency }
              let(:required_responses) { required }
              let(:received_responses) { received }

              it "retries at consistency #{retry_consistency.inspect}" do
                expect(decision).to be_a(Decisions::Retry)
                expect(decision.consistency).to eq(retry_consistency)
              end
            end
          end

          context("required_responses=1 received_responses=0") do
            let(:required_responses) { 1 }
            let(:received_responses) { 0 }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end

          context("received_responses=required_responses data_retrieved=false") do
            let(:data_retrieved)     { false }
            let(:received_responses) { required_responses }

            it 'retries at the same consistency level' do
              expect(decision).to be_a(Decisions::Retry)
              expect(decision.consistency).to eq(consistency)
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
            let(:retries) { 1 }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end

          ['SIMPLE', 'BATCH'].each do |type|
            context("write_type=#{type}") do
              let(:write_type) { type }

              it 'ignores' do
                expect(decision).to be_a(Decisions::Ignore)
              end
            end
          end

          context("write_type=BATCH_LOG") do
            let(:write_type) { 'BATCH_LOG' }

            it 'retries at the same consistency level' do
              expect(decision).to be_a(Decisions::Retry)
              expect(decision.consistency).to eq(consistency)
            end
          end

          context("write_type=UNLOGGED_BATCH") do
            let(:write_type) { 'UNLOGGED_BATCH' }

            [
              [7, 4, :all, :quorum],
              [7, 3, :all, :three],
              [7, 2, :all, :two],
              [7, 1, :all, :one],
            ].each do |(required, received, consistency, retry_consistency)|
              context("acks_required=#{required} acks_received=#{received} consistency=#{consistency.inspect}") do
                let(:consistency)   { consistency }
                let(:acks_required) { required }
                let(:acks_received) { received }

                it "retries at consistency #{retry_consistency.inspect}" do
                  expect(decision).to be_a(Decisions::Retry)
                  expect(decision.consistency).to eq(retry_consistency)
                end
              end
            end

            context("acks_required=1 acks_received=0") do
              let(:acks_required) { 1 }
              let(:acks_received) { 0 }

              it 'reraises' do
                expect(decision).to be_a(Decisions::Reraise)
              end
            end
          end

          context("all other write_types") do
            let(:write_type) { 'SOME OTHER TYPE' }

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
          let(:retries)           { 0 }

          let(:decision) do
            policy.unavailable(statement, consistency, replicas_required,
                               replicas_alive, retries)
          end


          context('second attempt') do
            let(:retries) { 1 }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end

          [
            [7, 4, :all, :quorum],
            [7, 3, :all, :three],
            [7, 2, :all, :two],
            [7, 1, :all, :one],
          ].each do |(required, received, consistency, retry_consistency)|
            context("replicas_required=#{required} replicas_alive=#{received} consistency=#{consistency.inspect}") do
              let(:consistency)       { consistency }
              let(:replicas_required) { required }
              let(:replicas_alive)    { received }

              it "retries at consistency #{retry_consistency.inspect}" do
                expect(decision).to be_a(Decisions::Retry)
                expect(decision.consistency).to eq(retry_consistency)
              end
            end
          end

          context("replicas_required=1 replicas_alive=0") do
            let(:replicas_required) { 1 }
            let(:replicas_alive) { 0 }

            it 'reraises' do
              expect(decision).to be_a(Decisions::Reraise)
            end
          end
        end
      end
    end
  end
end
