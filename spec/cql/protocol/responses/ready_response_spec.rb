# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe ReadyResponse do
      describe '.decode' do
        it 'returns a new instance' do
          unused_byte_buffer = nil
          described_class.decode(1, unused_byte_buffer, 0).should be_a(described_class)
        end
      end

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
