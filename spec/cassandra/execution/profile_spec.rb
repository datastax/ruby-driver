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
    describe Profile do
      let(:lbp) { double('lbp') }
      let(:lbp2) { double('lbp2') }
      let(:retry_policy) { double('retry_policy') }
      let(:retry_policy2) { double('retry_policy2') }

      before do
        [lbp, lbp2].each do |policy|
          allow(policy).to receive(:host_up)
          allow(policy).to receive(:host_down)
          allow(policy).to receive(:host_found)
          allow(policy).to receive(:host_lost)
          allow(policy).to receive(:setup)
          allow(policy).to receive(:teardown)
          allow(policy).to receive(:distance)
          allow(policy).to receive(:plan)
        end

        [retry_policy, retry_policy2].each do |policy|
          allow(policy).to receive(:read_timeout)
          allow(policy).to receive(:write_timeout)
          allow(policy).to receive(:unavailable)
        end
      end

      context :initialize do
        it 'should validate :load_balancing_policy option' do
          expect(Profile.new(load_balancing_policy: lbp).load_balancing_policy).to eq(lbp)
          ['junk', nil].each do |val|
            expect { Profile.new(load_balancing_policy: val) }.to raise_error(ArgumentError)
          end
        end

        it 'should validate :retry_policy option' do
          expect(Profile.new(retry_policy: retry_policy).retry_policy).to be(retry_policy)
          expect { Profile.new(retry_policy: 'junk') }.to raise_error(ArgumentError)
          expect { Profile.new(retry_policy: nil) }.to raise_error(ArgumentError)
        end

        it 'should validate :timeout' do
          ['a', -1, 0].each do |val|
            expect { Profile.new(timeout: val) }.to raise_error(ArgumentError)
          end
          [0.5, 38, nil].each do |val|
            expect(Profile.new(timeout: val).timeout).to eq(val)
          end
        end

        it 'should validate :consistency' do
          Cassandra::CONSISTENCIES.each do |c|
            expect(Profile.new(consistency: c).consistency).to eq(c)
          end
          expect { Profile.new(consistency: 'foo') }.to raise_error(ArgumentError)
        end

        it 'should fall back to system defaults for unspecified attributes' do
          p = Profile.new
          expect(p.load_balancing_policy).to be_a(LoadBalancing::Policies::TokenAware)
          expect(p.retry_policy).to be_a(Retry::Policies::Default)
          expect(p.timeout).to eq(12)
          expect(p.consistency).to be(:local_one)
        end

        it 'should support nil timeout' do
          expect(Profile.new(timeout: nil).timeout).to be_nil
        end
      end
    end
  end
end
