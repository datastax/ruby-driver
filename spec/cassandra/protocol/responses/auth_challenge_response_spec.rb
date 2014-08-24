# encoding: ascii-8bit

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
  module Protocol
    describe AuthChallengeResponse do
      describe '.decode' do
        it 'decodes the token' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x0cbingbongpong")
          response = described_class.decode(1, buffer, buffer.length)
          response.token.should == 'bingbongpong'
        end

        it 'decodes a nil token' do
          buffer = CqlByteBuffer.new("\xff\xff\xff\xff")
          response = described_class.decode(1, buffer, buffer.length)
          response.token.should be_nil
        end
      end

      describe '#to_s' do
        it 'returns a string with number of bytes in the token' do
          response = described_class.new('bingbongpong')
          response.to_s.should == 'AUTH_CHALLENGE 12'
        end
      end
    end
  end
end
