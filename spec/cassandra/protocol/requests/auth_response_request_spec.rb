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
#++

require 'spec_helper'


module Cassandra
  module Protocol
    describe AuthResponseRequest do
      describe '#write' do
        let(:encoder) { double('encoder') }

        it 'encodes an AUTH_RESPONSE request frame' do
          bytes = described_class.new('bingbongpong').write(CqlByteBuffer.new, 2, encoder)
          bytes.should eql_bytes("\x00\x00\x00\x0cbingbongpong")
        end

        it 'encodes a nil token' do
          bytes = described_class.new(nil).write(CqlByteBuffer.new, 2, encoder)
          bytes.should eql_bytes("\xff\xff\xff\xff")
        end
      end

      describe '#to_s' do
        it 'returns a string representation including the number of bytes in the token' do
          described_class.new('bingbongpong').to_s.should == 'AUTH_RESPONSE 12'
        end
      end

      describe '#eql?' do
        it 'is equal to another with the same token' do
          r1 = described_class.new('foo')
          r2 = described_class.new('foo')
          r1.should eql(r2)
        end

        it 'is not equal when the token is different' do
          r1 = described_class.new('foo')
          r2 = described_class.new('bar')
          r1.should_not eql(r2)
        end

        it 'is aliased as #==' do
          r1 = described_class.new('foo')
          r2 = described_class.new('foo')
          r1.should == r2
        end
      end

      describe '#hash' do
        it 'is the same when the token is the same' do
          r1 = described_class.new('foo')
          r2 = described_class.new('foo')
          r1.hash.should == r2.hash
        end

        it 'is not the same when the token is different' do
          r1 = described_class.new('foo')
          r2 = described_class.new('bar')
          r1.hash.should_not == r2.hash
        end
      end
    end
  end
end
