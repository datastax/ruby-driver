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
    describe RegisterRequest do
      describe '#write' do
        let(:encoder) { double('encoder') }

        it 'encodes a REGISTER request frame' do
          bytes = RegisterRequest.new('TOPOLOGY_CHANGE', 'STATUS_CHANGE').write(CqlByteBuffer.new, 1, encoder)
          bytes.should eql_bytes("\x00\x02\x00\x0fTOPOLOGY_CHANGE\x00\x0dSTATUS_CHANGE")
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = RegisterRequest.new('TOPOLOGY_CHANGE', 'STATUS_CHANGE')
          request.to_s.should == 'REGISTER ["TOPOLOGY_CHANGE", "STATUS_CHANGE"]'
        end
      end
    end
  end
end
