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
  module Execution
    describe(Options) do
      let(:load_balancing_policy) { double('lbp') }
      let(:lbp2) { double('lbp2') }
      let(:retry_policy) { double('retry_policy') }
      let(:base_options) {
        Options.new(timeout: 10, consistency: :one, load_balancing_policy: load_balancing_policy,
                    retry_policy: retry_policy)
      }

      before do
        [load_balancing_policy, lbp2].each do |policy|
          allow(policy).to receive(:host_up)
          allow(policy).to receive(:host_down)
          allow(policy).to receive(:host_found)
          allow(policy).to receive(:host_lost)
          allow(policy).to receive(:setup)
          allow(policy).to receive(:teardown)
          allow(policy).to receive(:distance)
          allow(policy).to receive(:plan)
        end

        allow(retry_policy).to receive(:read_timeout)
        allow(retry_policy).to receive(:write_timeout)
        allow(retry_policy).to receive(:unavailable)
      end

      context :initialize do
        it 'should yell if load-balancing policy is invalid' do
          ['junk', nil].each do |val|
            expect {
              Options.new(load_balancing_policy: val, retry_policy: retry_policy, consistency: :one)
            }.to raise_error(ArgumentError)
          end
        end

        it 'should yell if retry policy is invalid' do
          ['junk', nil].each do |val|
            expect {
              Options.new(load_balancing_policy: load_balancing_policy, retry_policy: val, consistency: :one)
            }.to raise_error(ArgumentError)
          end
        end
      end

      it 'should allow nil timeout to override base non-nil timeout option' do
        result = Options.new({timeout: nil}, base_options)
        expect(result.timeout).to be_nil
      end

      it 'should non-nil timeout to override base non-nil timeout option' do
        result = Options.new({timeout: 123}, base_options)
        expect(result.timeout).to eq(123)
      end

      it 'should not override base timeout if not specified' do
        result = Options.new({}, base_options)
        expect(result.timeout).to eq(10)
      end

      it 'should override with execution-profile and simple attribute' do
        profile = Profile.new(load_balancing_policy: lbp2, retry_policy: retry_policy, timeout: 21, consistency: :quorum)
        result = base_options.override(profile, timeout: 42)
        expect(result.load_balancing_policy).to be(lbp2)
        expect(result.retry_policy).to be(retry_policy)
        expect(result.consistency).to be(:quorum)
        expect(result.timeout).to eq(42)
      end
    end
  end
end
