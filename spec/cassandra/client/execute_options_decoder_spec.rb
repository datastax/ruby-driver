# encoding: utf-8

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

require 'spec_helper'


module Cassandra
  module Client
    describe ExecuteOptionsDecoder do
      let :decoder do
        described_class.new(:two)
      end

      describe '#decode_options' do
        it 'returns the default consistency' do
          options = decoder.decode_options({})
          options.should include(consistency: :two)
        end

        it 'returns the default consistency when given no options' do
          options = decoder.decode_options
          options.should include(consistency: :two)
        end

        it 'returns the default consistency when given nil' do
          options = decoder.decode_options(nil)
          options.should include(consistency: :two)
        end

        it 'uses the consistency given in the options' do
          options = decoder.decode_options(consistency: :three)
          options.should include(consistency: :three)
        end

        it 'uses the consistency given as a symbol' do
          options = decoder.decode_options(:three)
          options.should include(consistency: :three)
        end

        it 'defaults to no serial consistency' do
          options = decoder.decode_options({})
          options.should_not have_key(:serial_consistency)
        end

        it 'uses the serial consistency given in the options' do
          options = decoder.decode_options(serial_consistency: :local_serial)
          options.should include(serial_consistency: :local_serial)
        end

        it 'defaults to no tracing' do
          options = decoder.decode_options({})
          options.should_not have_key(:trace)
        end

        it 'uses the tracing value given in the options' do
          options = decoder.decode_options(trace: true)
          options.should include(trace: true)
          options = decoder.decode_options(trace: false)
          options.should include(trace: false)
        end

        it 'defaults to no timeout' do
          options = decoder.decode_options({})
          options.should_not have_key(:timeout)
        end

        it 'uses the timeout value given in the options' do
          options = decoder.decode_options(timeout: 3)
          options.should include(timeout: 3)
        end

        context 'when called with multiple options' do
          it 'merges the options' do
            options = decoder.decode_options({:tracing => true, :timeout => 3}, {:consistency => :quorum, :timeout => 4})
            options.should eql(tracing: true, timeout: 4, consistency: :quorum)
          end

          it 'uses the default consistency' do
            options = decoder.decode_options({tracing: true, timeout: 3}, {:timeout => 4})
            options.should eql(tracing: true, timeout: 4, consistency: :two)
          end

          it 'accepts nil' do
            options = decoder.decode_options(nil, {tracing: true, timeout: 3}, nil, {:timeout => 4})
            options.should eql(tracing: true, timeout: 4, consistency: :two)
          end

          it 'accepts consistencies given as symbols' do
            options = decoder.decode_options({tracing: true, timeout: 3}, :quorum)
            options.should eql(tracing: true, timeout: 3, consistency: :quorum)
          end
        end
      end
    end
  end
end