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
  class Uuid
    describe Generator do
      let :generator do
        described_class.new((SecureRandom.random_number(2**47) | 0x010000000000), SecureRandom.random_number(65536), double(now: clock))
      end

      let :clock do
        ::Time.at(1370771820, 329394)
      end

      describe '#now' do
        it 'returns a UUID generated from the current time' do
          x = generator.now
          x.to_time.to_i.should == 1370771820
          x.to_time.usec.should == 329394
        end

        it 'returns unique IDs even when called within a time shorter than the clock resolution' do
          x1 = generator.now
          x2 = generator.now
          clock.stub(:usec).and_return(329394 + 1)
          x3 = generator.now
          x1.should_not == x2
          x2.should_not == x3
        end

        it 'creates a pseudo random clock ID' do
          str = generator.now.to_s.split('-')[3]
          str.should_not === '0000'
        end

        it 'uses the clock ID for all generated UUIDs' do
          str1 = generator.now.to_s.split('-')[3]
          str2 = generator.now.to_s.split('-')[3]
          str3 = generator.now.to_s.split('-')[3]
          str1.should == str2
          str2.should == str3
        end

        it 'creates a new clock ID when the clock has moved backwards' do
          str1 = generator.now.to_s.split('-')[3]
          clock.stub(:to_i).and_return(1370771820 - 5)
          str2 = generator.now.to_s.split('-')[3]
          str1.should_not == str2
        end

        it 'creates a pseudo random node ID' do
          str = generator.now.to_s.split('-')[4]
          str.should_not == '000000000000'
        end

        it 'uses the node ID for all generated UUIDs' do
          str1 = generator.now.to_s.split('-')[4]
          str2 = generator.now.to_s.split('-')[4]
          str3 = generator.now.to_s.split('-')[4]
          str1.should == str2
          str2.should == str3
        end

        it 'sets the multicast bit of the node ID (so that it does not conflict with valid MAC addresses)' do
          x = generator.now.value & 0x010000000000
          x.should == 0x010000000000
        end

        it 'generates a version 1, variant 1 UUID' do
          x = generator.at(clock)
          (x.value & 0x10008000000000000000).should == 0x10008000000000000000
        end
      end

      describe '#at' do
        it 'returns a UUID for the specified time with a bit of random jitter' do
          x = generator.at(clock)
          x.to_time.to_i.should == 1370771820
          x.to_time.usec.should be > 329394
        end

        it 'returns a UUID for the specified time with an offset' do
          x = generator.at(clock, 8)
          x.to_time.to_i.should == 1370771820
          x.to_time.usec.should == 329394 + 8
        end

        it 'returns a UUID for the specified timestamp with a bit of random jitter' do
          x = generator.at(1370771820, 329394)
          x.to_time.to_i.should == 1370771820
          x.to_time.usec.should be > 329394
        end

        it 'returns a UUID for the specified timestamp with an offset' do
          x = generator.at(1370771820, 329394, 8)
          x.to_time.to_i.should == 1370771820
          x.to_time.usec.should == 329394 + 8
        end
      end

      context 'when specifying a custom clock ID' do
        it 'uses the lower 14 bits of the specified clock ID' do
          g = described_class.new(0, 0x2bad, double(now: clock))
          (g.now.value >> 48 & 0x3fff).should == 0x2bad
        end

        it 'ensures that the high bit of the clock ID is 1 (the variant)' do
          g = described_class.new(0, 0x2bad, double(now: clock))
          (g.now.value >> 60 & 0b1000).should == 0b1000
        end

        it 'generates a new random clock ID if time has moved backwards' do
          g = described_class.new(0, 0x2bad, double(now: clock))
          str1 = g.now.to_s.split('-')[3]
          clock.stub(:to_i).and_return(1370771820 - 2)
          str2 = g.now.to_s.split('-')[3]
          str1.should_not == str2
        end
      end

      context 'when specifying a custom node ID' do
        it 'uses the lower 48 bits of the specified node ID' do
          g = described_class.new(0xd00b1ed00b1ed00b, 0x0000, double(now: clock))
          g.now.to_s.should end_with('00-1ed00b1ed00b')
        end

        it 'does not modify the multicast bit' do
          g = described_class.new(0x000000000000, 0x0000, double(now: clock))
          g.now.to_s.should end_with('00-000000000000')
        end
      end
    end
  end
end
