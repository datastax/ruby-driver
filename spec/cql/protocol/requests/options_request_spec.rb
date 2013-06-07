# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe OptionsRequest do
      describe '#encode_frame' do
        it 'encodes an OPTIONS request frame' do
          bytes = OptionsRequest.new.encode_frame(3)
          bytes.should == "\x01\x00\x03\x05\x00\x00\x00\x00"
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = OptionsRequest.new
          request.to_s.should == 'OPTIONS'
        end
      end
    end
  end
end