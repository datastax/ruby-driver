# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe Request do
      describe '#encode_frame' do
        it 'returns a rendered request frame for the specified channel' do
          frame = OptionsRequest.new.encode_frame(3)
          frame.to_s.should start_with("\x01\x00\x03\x05\x00\x00\x00\x00")
        end
      end
    end
  end
end