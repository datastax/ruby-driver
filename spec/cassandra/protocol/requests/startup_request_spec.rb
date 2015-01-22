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
    describe StartupRequest do
      describe '#initialize' do
        it 'raises an error if the CQL version is not specified' do
          expect { described_class.new(nil) }.to raise_error(ArgumentError)
        end
      end

      describe '#compressable?' do
        it 'is not compressable' do
          described_class.new('3.1.1').should_not be_compressable
        end
      end

      describe '#write' do
        let(:encoder) { double('encoder') }

        it 'encodes a STARTUP request frame' do
          bytes = StartupRequest.new('3.0.0', 'snappy').write(CqlByteBuffer.new, 1, encoder)
          bytes.should eql_bytes("\x00\x02\x00\x0bCQL_VERSION\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x06snappy")
        end

        it 'defaults to no compression' do
          bytes = StartupRequest.new('3.1.1').write(CqlByteBuffer.new, 1, encoder)
          bytes.should eql_bytes("\x00\x01\x00\x0bCQL_VERSION\x00\x053.1.1")
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = StartupRequest.new('3.0.0')
          request.to_s.should == 'STARTUP {"CQL_VERSION"=>"3.0.0"}'
        end
      end
    end
  end
end
