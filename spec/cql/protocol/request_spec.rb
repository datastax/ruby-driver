# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe Request do
      describe '#encode_frame' do
        it 'returns a rendered request frame for the specified channel' do
          frame = PrepareRequest.new('SELECT * FROM things').encode_frame(3)
          frame.to_s.should == "\x01\x00\x03\x09\x00\x00\x00\x18\x00\x00\x00\x14SELECT * FROM things"
        end
      end
    end
  end
end