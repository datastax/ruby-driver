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

      describe '.change_stream_id' do
        it 'changes the stream ID byte' do
          buffer = ByteBuffer.new("\x01\x00\x03\x02\x00\x00\x00\x00")
          described_class.change_stream_id(99, buffer)
          buffer.discard(2)
          buffer.read_byte.should == 99
        end

        it 'changes the stream ID byte of the frame starting at the specified offset' do
          buffer = ByteBuffer.new("hello foo\x01\x00\x03\x02\x00\x00\x00\x00")
          described_class.change_stream_id(99, buffer, 9)
          buffer.discard(11)
          buffer.read_byte.should == 99
        end
      end
    end
  end
end