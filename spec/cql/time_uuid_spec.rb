# encoding: utf-8

require 'spec_helper'


module Cql
  describe TimeUuid do
    let :time do
      Time.utc(2013, 6, 7, 8, 9, 10)
    end

    describe '.from_time' do
      it 'can be created from a Time instance' do
        x = described_class.from_time(time, 0, 0)
        x.to_s.should start_with('88401700-cf49-11e2-')
      end

      it 'can be created with a clock ID' do
        x = described_class.from_time(time, 0x0bad, 0)
        x.to_s.should match(/^.{8}-.{4}-.{4}-.bad/)
      end

      it 'uses the lower 14 bits of the clock ID' do
        x = described_class.from_time(time, 0xffffffcafe, 0)
        x.to_s.should match(/^.{8}-.{4}-11e2-.afe/)
      end

      it 'ensures that the high bit of the clock ID is 1' do
        x = described_class.from_time(time, 0x1337, 0)
        x.to_s.should match(/^.{8}-.{4}-.{4}-9337/)
      end

      it 'can be created with a node ID' do
        x = described_class.from_time(time, 0xcafe, 0x68656c6c6f20)
        x.to_s.should match(/^.{8}-.{4}-.{4}-.{4}-68656c6c6f20$/)
      end

      it 'uses the lower 64 bits of the node ID' do
        x = described_class.from_time(time, 0xcafe, 0xffffffffff68656c6c6f20)
        x.to_s.should match(/^.{8}-.{4}-.{4}-8afe-68656c6c6f20$/)
      end

      it 'is a version 1, variant 1 UUID' do
        x = described_class.from_time(time, 0, 0)
        (x.value & 0x10008000000000000000).should == 0x10008000000000000000
      end
    end

    describe '#to_time' do
      it 'returns a Time' do
        x = TimeUuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66')
        x.to_time.should be > Time.utc(2013, 6, 9, 8, 45, 57)
        x.to_time.should be < Time.utc(2013, 6, 9, 8, 45, 58)
      end
    end

    describe '#value' do
      it 'returns the numeric value' do
        x = described_class.from_time(time, 0, 0)
        x.value.should be > 0x88401700cf4900000000000000000000
        x.value.should be < 0x88401700cf4a00000000000000000000
      end
    end
  end

  describe TimeUuid::Generator do
    let :generator do
      described_class.new(nil, nil, stub(now: clock))
    end

    let :clock do
      stub(:clock, to_i: 1370771820, usec: 329393)
    end

    before do
      generator
      clock.stub(:usec).and_return(329394)
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
  end
end