# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe SetKeyspaceResultResponse do
      describe '.decode' do
        let :response do
          described_class.decode(1, CqlByteBuffer.new("\x00\x06system"), 8)
        end

        it 'decodes the keyspace' do
          response.keyspace.should == 'system'
        end
      end

      describe '#void?' do
        it 'is not void' do
          response = described_class.new('system', nil)
          response.should_not be_void
        end
      end

      describe '#to_s' do
        it 'returns a string with the keyspace' do
          response = described_class.new('system', nil)
          response.to_s.should == 'RESULT SET_KEYSPACE "system"'
        end
      end
    end
  end
end
