# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe ErrorResponse do
      describe '.decode' do
        let :response do
          buffer = CqlByteBuffer.new("\x00\x00\x00\n\x00PProvided version 4.0.0 is not supported by this server (supported: 2.0.0, 3.0.0)")
          described_class.decode(1, buffer, buffer.length)
        end

        it 'decodes the error code' do
          response.code.should == 10
        end

        it 'decodes the error message' do
          response.message.should == 'Provided version 4.0.0 is not supported by this server (supported: 2.0.0, 3.0.0)'
        end

        it 'decodes error frames with details' do
          buffer = CqlByteBuffer.new("\x00\x00\x11\x00\x000Operation timed out - received only 0 responses.\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\tBATCH_LOG")
          response = described_class.decode(1, buffer, buffer.length)
          response.details.should_not be_nil
        end
      end

      describe '#to_s' do
        it 'returns a string with the error code and the message' do
          response = described_class.new(0xffff, 'This is an error')
          response.to_s.should == 'ERROR 0xFFFF "This is an error"'
        end
      end
    end
  end
end
