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
  module Execution
    describe(ProfileManager) do
      let(:cluster) { double('cluster') }
      let(:host) { double('host') }
      let(:lbp1) { double('lbp1') }
      let(:lbp2) { double('lbp2') }
      let(:lbp3) { double('lbp3') }
      let(:retry_policy) { double('retry_policy') }
      let(:profile1) { Profile.new(load_balancing_policy: lbp1) }
      let(:profile2) { Profile.new(load_balancing_policy: lbp2) }
      let(:profile3) { Profile.new(load_balancing_policy: lbp3) }
      let(:profile5) { Profile.new(load_balancing_policy: lbp1) }
      let(:default_profile) {
        Profile.new(load_balancing_policy: lbp1, retry_policy: retry_policy, consistency: :quorum, timeout: 12)
      }
      let(:subject) {
        ProfileManager.new(default_profile, {p1: profile1, p2: profile2, p3: profile3, p5: profile5})
      }
      let(:subject_with_custom_default) {
        ProfileManager.new(default_profile, {default: profile2})
      }

      before do
        [lbp1, lbp2, lbp3].each do |policy|
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

      it 'should respect custom default profile' do
        expect(subject_with_custom_default.profiles[:default]).to be(profile2)
      end

      it 'should return unique list of lbps' do
        expect(subject.load_balancing_policies.size).to eq(3)
        expect(subject.load_balancing_policies.include?(lbp1)).to eq(true)
      end

      context :distance do
        it 'should return :local if any lbp shows a :local distance' do
          expect(lbp1).to receive(:distance).with(host).and_return(:local)
          expect(lbp2).to receive(:distance).with(host).and_return(:remote)
          expect(lbp3).to receive(:distance).with(host).and_return(:ignore)

          expect(subject.distance(host)).to be(:local)
        end

        it 'should return :remote if any lbp shows a :remote distance and none show :local' do
          expect(lbp1).to receive(:distance).with(host).and_return(:ignore)
          expect(lbp2).to receive(:distance).with(host).and_return(:remote)
          expect(lbp3).to receive(:distance).with(host).and_return(:ignore)

          expect(subject.distance(host)).to be(:remote)
        end

        it 'should return :ignore if all lbps ignore the host' do
          expect(lbp1).to receive(:distance).with(host).and_return(:ignore)
          expect(lbp2).to receive(:distance).with(host).and_return(:ignore)
          expect(lbp3).to receive(:distance).with(host).and_return(:ignore)

          expect(subject.distance(host)).to be(:ignore)
        end
      end
    end
  end
end
