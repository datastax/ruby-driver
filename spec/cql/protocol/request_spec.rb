# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe Request do
      describe '#encode_frame' do
        it 'returns a rendered request frame for the specified channel' do
          encoded_frame = PrepareRequest.new('SELECT * FROM things').encode_frame(3)
          encoded_frame.to_s.should == "\x01\x00\x03\x09\x00\x00\x00\x18\x00\x00\x00\x14SELECT * FROM things"
        end

        it 'appends a rendered request frame to the specified buffer' do
          buffer = ByteBuffer.new('hello')
          encoded_frame = PrepareRequest.new('SELECT * FROM things').encode_frame(3, buffer)
          buffer.to_s.should == "hello\x01\x00\x03\x09\x00\x00\x00\x18\x00\x00\x00\x14SELECT * FROM things"
        end

        it 'returns the specified buffer' do
          buffer = ByteBuffer.new('hello')
          encoded_frame = PrepareRequest.new('SELECT * FROM things').encode_frame(3, buffer)
          encoded_frame.should equal(buffer)
        end
      end
    end
  end
end