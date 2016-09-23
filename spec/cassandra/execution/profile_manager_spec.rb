# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
      let(:registry) { FakeClusterRegistry.new(['127.0.0.1', '127.0.0.2']) }
      let(:lbp1) { FakeLoadBalancingPolicy.new(registry) }
      let(:lbp2) { FakeLoadBalancingPolicy.new(registry) }
      let(:lbp3) { FakeLoadBalancingPolicy.new(registry) }
      let(:profile1) { Profile.new(load_balancing_policy: lbp1) }
      let(:profile2) { Profile.new(load_balancing_policy: lbp2) }
      let(:profile3) { Profile.new(load_balancing_policy: lbp3) }
      let(:profile4) { Profile.new }
      let(:subject) {
        ProfileManager.new(p1: profile1, p2: profile2, p3: profile3, p4: profile4)
      }

      it 'should delegate :setup to underlying load-balancing policies' do
        expect(lbp1).to receive(:setup).with(cluster)
        expect(lbp2).to receive(:setup).with(cluster)
        expect(lbp3).to receive(:setup).with(cluster)
        subject.setup(cluster)
      end

      it 'should delegate :teardown to underlying load-balancing policies' do
        expect(lbp1).to receive(:teardown).with(cluster)
        expect(lbp2).to receive(:teardown).with(cluster)
        expect(lbp3).to receive(:teardown).with(cluster)
        subject.teardown(cluster)
      end

      [:host_up, :host_down, :host_found, :host_lost].each do |event|
        it "should delegate :#{event} to underlying load-balancing policies" do
          expect(lbp1).to receive(event).with(host)
          expect(lbp2).to receive(event).with(host)
          expect(lbp3).to receive(event).with(host)
          subject.send(event, host)
        end
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
