# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe AuthChallengeResponse do
      describe '.decode' do
        it 'decodes the token' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x0cbingbongpong")
          response = described_class.decode(1, buffer, buffer.length)
          response.token.should == 'bingbongpong'
        end

        it 'decodes a nil token' do
          buffer = CqlByteBuffer.new("\xff\xff\xff\xff")
          response = described_class.decode(1, buffer, buffer.length)
          response.token.should be_nil
        end
      end

      describe '#to_s' do
        it 'returns a string with number of bytes in the token' do
          response = described_class.new('bingbongpong')
          response.to_s.should == 'AUTH_CHALLENGE 12'
        end
      end
    end
  end
end
