# encoding: utf-8

require 'spec_helper'


module Cql
  describe Uuid do
    describe '#initialize' do
      it 'can be created from a string' do
        Uuid.new('a4a70900-24e1-11df-8924-001ff3591711').to_s.should == 'a4a70900-24e1-11df-8924-001ff3591711'
      end

      it 'can be created from a string without hyphens' do
        Uuid.new('a4a7090024e111df8924001ff3591711').to_s.should == 'a4a70900-24e1-11df-8924-001ff3591711'
      end

      it 'raises an error if the string is shorter than 32 chars' do
        expect { Uuid.new('a4a7090024e111df8924001ff359171') }.to raise_error(ArgumentError)
      end

      it 'raises an error if the string is longer than 32 chars' do
        expect { Uuid.new('a4a7090024e111df8924001ff35917111') }.to raise_error(ArgumentError)
      end

      it 'raises an error if the string is not a hexadecimal number' do
        expect { Uuid.new('a4a7090024e111df8924001ff359171x') }.to raise_error(ArgumentError)
      end

      it 'can be created from a number' do
        Uuid.new(276263553384940695775376958868900023510).to_s.should == 'cfd66ccc-d857-4e90-b1e5-df98a3d40cd6'.force_encoding(::Encoding::ASCII)
      end
    end

    describe '#eql?' do
      it 'is equal to another Uuid with the same value' do
        Uuid.new(276263553384940695775376958868900023510).should eql(Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6'))
      end

      it 'is not equal when anything other than a Uuid is passed' do
        [nil, 123, 'test'].each { |v| Uuid.new(276263553384940695775376958868900023510).should_not eql(v) }
      end

      it 'aliases #== to #eql?' do
        Uuid.new(276263553384940695775376958868900023510).should == Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6')
      end
    end

    describe '#to_s' do
      it 'returns a UUID standard format' do
        Uuid.new('a4a70900-24e1-11df-8924-001ff3591711').to_s.should == 'a4a70900-24e1-11df-8924-001ff3591711'
      end
    end

    describe '#hash' do
      it 'calculates a 64 bit hash of the UUID' do
        h = Uuid.new(276263553384940695775376958868900023510).hash
        h.should be < 2**63
        h.should be > -2**63
      end

      it 'has the same hash code when #eql?' do
        uuid1 = Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        uuid2 = Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        uuid1.hash.should == uuid2.hash
      end

      it 'has a different hash when not #eql?' do
        uuid1 = Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        uuid2 = Uuid.new('b4a70900-24e1-11df-8924-001ff3591711')
        uuid1.hash.should_not == uuid2.hash
      end
    end

    describe '#value' do
      it 'returns the numeric value' do
        Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6').value.should == 276263553384940695775376958868900023510
      end
    end
  end
end
