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
    describe(Options) do
      let(:base_options) { Options.new(timeout: 10, consistency: :one) }
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
    end
  end
end
