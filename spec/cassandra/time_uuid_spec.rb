# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
  describe TimeUuid do
    describe '#to_time' do
      it 'returns a Time' do
        x = TimeUuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66')
        x.to_time.should be > Time.utc(2013, 6, 9, 8, 45, 57)
        x.to_time.should be < Time.utc(2013, 6, 9, 8, 45, 58)
      end
    end

    describe '#<=>' do
      let :generator do
        Uuid::Generator.new
      end

      let :uuids do
        [
          generator.at(Time.utc(2014, 5,  1,  2, 3, 4, 1), 0),
          generator.at(Time.utc(2014, 5,  1,  2, 3, 4, 2), 0),
          generator.at(Time.utc(2014, 5,  1,  2, 3, 5, 0), 0),
          generator.at(Time.utc(2014, 5, 11, 14, 3, 4, 0), 0),
          generator.at(Time.utc(2014, 5, 20,  2, 3, 4, 0), 0),
          generator.at(Time.utc(2014, 6,  7,  2, 3, 4, 0), 0),
        ]
      end

      it 'sorts by the time component' do
        uuids.shuffle.sort.should == uuids
      end

      it 'allows comparison of UUID and TimeUUID' do
        x = generator.now
        y = Uuid.new(x.value)
        x.should == y
      end

      it 'allows comparison of TimeUUID and nil' do
        x = generator.now
        y = nil
        lambda { x.should_not == y }.should_not raise_error
      end
    end
  end
end
