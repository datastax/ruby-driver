# encoding: utf-8

require 'spec_helper'


module Cql
  describe Uuid do
    describe '#initialize' do
      it 'can be created from a string' do
        Uuid.new('a4a70900-24e1-11df-8924-001ff3591711').to_s.should == 'a4a70900-24e1-11df-8924-001ff3591711'
      end

      it 'can be created from a number' do
        Uuid.new(276263553384940695775376958868900023510).to_s.should == 'cfd66ccc-d857-4e90-b1e5-df98a3d40cd6'.force_encoding(::Encoding::ASCII)
      end
    end

    describe '#eql?' do
      it 'is equal to another Uuid with the same value' do
        Uuid.new(276263553384940695775376958868900023510).should eql(Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6'))
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

    describe '#value' do
      it 'returns the numeric value' do
        Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6').value.should == 276263553384940695775376958868900023510
      end
    end
  end
end