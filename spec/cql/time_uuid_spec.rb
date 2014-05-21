# encoding: utf-8

require 'spec_helper'


module Cql
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
        TimeUuid::Generator.new
      end

      let :uuids do
        [
          generator.from_time(Time.utc(2014, 5,  1,  2, 3, 4, 1), 0),
          generator.from_time(Time.utc(2014, 5,  1,  2, 3, 4, 2), 0),
          generator.from_time(Time.utc(2014, 5,  1,  2, 3, 5, 0), 0),
          generator.from_time(Time.utc(2014, 5, 11, 14, 3, 4, 0), 0),
          generator.from_time(Time.utc(2014, 5, 20,  2, 3, 4, 0), 0),
          generator.from_time(Time.utc(2014, 6,  7,  2, 3, 4, 0), 0),
        ]
      end

      it 'sorts by the time component' do
        uuids.shuffle.sort.should == uuids
      end
    end
  end

  describe TimeUuid::Generator do
    let :generator do
      described_class.new(nil, nil, double(now: clock))
    end

    let :clock do
      double(:clock, to_i: 1370771820, usec: 329394)
    end

    describe '#next' do
      it 'returns a UUID generated from the current time' do
        x = generator.next
        x.to_time.to_i.should == 1370771820
        x.to_time.usec.should == 329394
      end

      it 'returns unique IDs even when called within a time shorter than the clock resolution' do
        x1 = generator.next
        x2 = generator.next
        clock.stub(:usec).and_return(329394 + 1)
        x3 = generator.next
        x1.should_not == x2
        x2.should_not == x3
      end

      it 'creates a pseudo random clock ID' do
        str = generator.next.to_s.split('-')[3]
        str.should_not === '0000'
      end

      it 'uses the clock ID for all generated UUIDs' do
        str1 = generator.next.to_s.split('-')[3]
        str2 = generator.next.to_s.split('-')[3]
        str3 = generator.next.to_s.split('-')[3]
        str1.should == str2
        str2.should == str3
      end

      it 'creates a new clock ID when the clock has moved backwards' do
        str1 = generator.next.to_s.split('-')[3]
        clock.stub(:to_i).and_return(1370771820 - 5)
        str2 = generator.next.to_s.split('-')[3]
        str1.should_not == str2
      end

      it 'creates a pseudo random node ID' do
        str = generator.next.to_s.split('-')[4]
        str.should_not == '000000000000'
      end

      it 'uses the node ID for all generated UUIDs' do
        str1 = generator.next.to_s.split('-')[4]
        str2 = generator.next.to_s.split('-')[4]
        str3 = generator.next.to_s.split('-')[4]
        str1.should == str2
        str2.should == str3
      end

      it 'sets the multicast bit of the node ID (so that it does not conflict with valid MAC addresses)' do
        x = generator.next.value & 0x010000000000
        x.should == 0x010000000000
      end

      it 'generates a version 1, variant 1 UUID' do
        x = generator.from_time(clock)
        (x.value & 0x10008000000000000000).should == 0x10008000000000000000
      end
    end

    describe '#from_time' do
      it 'returns a UUID for the specified time with a bit of random jitter' do
        x = generator.from_time(clock)
        x.to_time.to_i.should == 1370771820
        x.to_time.usec.should be > 329394
      end

      it 'returns a UUID for the specified time with an offset' do
        x = generator.from_time(clock, 8)
        x.to_time.to_i.should == 1370771820
        x.to_time.usec.should == 329394 + 8
      end
    end

    context 'when specifying a custom clock ID' do
      it 'uses the lower 14 bits of the specified clock ID' do
        g = described_class.new(nil, 0x2bad, double(now: clock))
        (g.next.value >> 48 & 0x3fff).should == 0x2bad
      end

      it 'ensures that the high bit of the clock ID is 1 (the variant)' do
        g = described_class.new(nil, 0x2bad, double(now: clock))
        (g.next.value >> 60 & 0b1000).should == 0b1000
      end

      it 'generates a new random clock ID if time has moved backwards' do
        g = described_class.new(nil, 0x2bad, double(now: clock))
        str1 = g.next.to_s.split('-')[3]
        clock.stub(:to_i).and_return(1370771820 - 2)
        str2 = g.next.to_s.split('-')[3]
        str1.should_not == str2
      end
    end

    context 'when specifying a custom node ID' do
      it 'uses the lower 48 bits of the specified node ID' do
        g = described_class.new(0xd00b1ed00b1ed00b, 0x0000, double(now: clock))
        g.next.to_s.should end_with('00-1ed00b1ed00b')
      end

      it 'does not modify the multicast bit' do
        g = described_class.new(0x000000000000, 0x0000, double(now: clock))
        g.next.to_s.should end_with('00-000000000000')
      end
    end
  end
end