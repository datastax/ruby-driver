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
    describe OptionsRequest do
      describe '#compressable?' do
        it 'is not compressable' do
          described_class.new.should_not be_compressable
        end
      end

      describe '#write' do
        let(:encoder) { double('encoder') }

        it 'encodes an OPTIONS request frame (i.e. an empty body)' do
          bytes = OptionsRequest.new.write(CqlByteBuffer.new, 1, encoder)
          bytes.should be_empty
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = OptionsRequest.new
          request.to_s.should == 'OPTIONS'
        end
      end
    end
  end
end
