# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe StatusChangeEventResponse do
      describe '.decode' do
        let :response do
          buffer = CqlByteBuffer.new("\x00\x04DOWN\x04\x00\x00\x00\x00\x00\x00#R")
          described_class.decode(1, buffer, buffer.length)
        end

        it 'decodes the change' do
          response.change.should == 'DOWN'
        end

        it 'decodes the address' do
          response.address.should == IPAddr.new('0.0.0.0')
        end

        it 'decodes the port' do
          response.port.should == 9042
        end
      end

      describe '#to_s' do
        it 'returns a string that includes the change, address and port' do
          response = described_class.new('DOWN', IPAddr.new('0.0.0.0'), 9042)
          response.to_s.should == 'EVENT STATUS_CHANGE DOWN 0.0.0.0:9042'
        end
      end
    end
  end
end
