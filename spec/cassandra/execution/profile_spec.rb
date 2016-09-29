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
    describe(Profile) do
      let(:profile1) { Profile.new(load_balancing_policy: lbp1) }
      let(:profile2) { Profile.new(load_balancing_policy: lbp2) }
      let(:profile3) { Profile.new(load_balancing_policy: lbp3) }
      let(:profile4) { Profile.new }

      context :timeout do
        it 'should return default timeout value if unspecified' do
          expect(Profile.new.timeout).to eq(12)
        end

        it 'should return nil if set with nil' do
          expect(Profile.new(timeout: nil).timeout).to be_nil
        end

        it 'should return timeout value if set with a real value' do
          expect(Profile.new(timeout: 6).timeout).to eq(6)
        end
      end

      context :merge_from do
        let(:lbp) { double('lbp') }
        let(:lbp2) { double('lbp2') }
        let(:retry_policy) { double('retry_policy') }
        let(:retry_policy2) { double('retry_policy2') }
        let(:default_profile) { Profile.new(load_balancing_policy: lbp, retry_policy: retry_policy, consistency: :one,
                                            timeout: 10)}
        let(:profile) {
          Profile.new(load_balancing_policy: lbp2, retry_policy: retry_policy2, consistency: :quorum, timeout: 23)
        }
        let(:empty_profile) { Profile.new }
        it 'should accept all attributes from parent profile if it has no attributes itself' do
          expect(Profile.new.merge_from(default_profile)).to eq(default_profile)
        end

        it 'should ignore all attributes from parent profile if it is fully specified itself' do
          expect(profile.merge_from(default_profile)).to eq(profile)
        end

        it 'should respect nil timeout in parent' do
          parent_profile = Profile.new(timeout: nil)
          expect(empty_profile.timeout).to eq(12)
          expect(empty_profile.merge_from(parent_profile).timeout).to be_nil
        end

        it 'should preserve its nil timeout when parent timeout is not nil' do
          profile = Profile.new(timeout: nil)
          expect(profile.timeout).to be_nil
          expect(profile.merge_from(default_profile).timeout).to be_nil
        end
      end
    end
  end
end
