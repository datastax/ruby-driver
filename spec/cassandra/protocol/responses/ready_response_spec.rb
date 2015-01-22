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
    describe ReadyResponse do
      describe '#to_s' do
        it 'returns a string' do
          described_class.new.to_s.should == 'READY'
        end
      end

      describe '#eql?' do
        it 'is equal to all other ready responses' do
          described_class.new.should eql(described_class.new)
        end

        it 'aliased as ==' do
          described_class.new.should == described_class.new
        end
      end

      describe '#hash' do
        it 'has the same hash code as all other ready responses' do
          described_class.new.hash.should == described_class.new.hash
        end
      end
    end
  end
end
