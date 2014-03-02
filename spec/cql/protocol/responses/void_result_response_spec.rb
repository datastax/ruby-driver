# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe VoidResultResponse do
      describe '.decode' do
        it 'returns a new instance' do
          unused_byte_buffer = nil
          described_class.decode(1, unused_byte_buffer, 0).should be_a(described_class)
        end
      end

      describe '#void?' do
        it 'is void' do
          described_class.new(nil).should be_void
        end
      end

      describe '#to_s' do
        it 'returns a string' do
          described_class.new(nil).to_s.should == 'RESULT VOID'
        end
      end
    end
  end
end
