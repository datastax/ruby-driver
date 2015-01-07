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
    describe StatusChangeEventResponse do
      describe '#to_s' do
        it 'returns a string that includes the change, address and port' do
          response = described_class.new('DOWN', IPAddr.new('0.0.0.0'), 9042)
          response.to_s.should == 'EVENT STATUS_CHANGE DOWN 0.0.0.0:9042'
        end
      end
    end
  end
end
