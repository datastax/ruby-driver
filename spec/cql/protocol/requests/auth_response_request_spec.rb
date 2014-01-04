# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe AuthResponseRequest do
      describe '#write' do
        it 'encodes an AUTH_RESPONSE request frame' do
          bytes = described_class.new('bingbongpong').write(2, '')
          bytes.should == "\x00\x00\x00\x0cbingbongpong"
        end

        it 'encodes a nil token' do
          bytes = described_class.new(nil).write(2, '')
          bytes.should == "\xff\xff\xff\xff"
        end
      end

      describe '#to_s' do
        it 'returns a string representation including the number of bytes in the token' do
          described_class.new('bingbongpong').to_s.should == 'AUTH_RESPONSE 12'
        end
      end
    end
  end
end