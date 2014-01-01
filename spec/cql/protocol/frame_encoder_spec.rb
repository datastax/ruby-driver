# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe FrameEncoder do
      let :encoder do
        described_class.new
      end

      describe '#encode_frame' do
        it 'returns a rendered request frame for the specified channel' do
          request = PrepareRequest.new('SELECT * FROM things')
          stream_id = 3
          encoded_frame = encoder.encode_frame(request, stream_id)
          encoded_frame.to_s.should == "\x01\x00\x03\x09\x00\x00\x00\x18\x00\x00\x00\x14SELECT * FROM things"
        end

        it 'appends a rendered request frame to the specified buffer' do
          buffer = ByteBuffer.new('hello')
          request = PrepareRequest.new('SELECT * FROM things')
          stream_id = 3
          encoded_frame = encoder.encode_frame(request, stream_id, buffer)
          buffer.to_s.should == "hello\x01\x00\x03\x09\x00\x00\x00\x18\x00\x00\x00\x14SELECT * FROM things"
        end

        it 'returns the specified buffer' do
          buffer = ByteBuffer.new('hello')
          request = PrepareRequest.new('SELECT * FROM things')
          stream_id = 3
          encoded_frame = encoder.encode_frame(request, stream_id, buffer)
          encoded_frame.should equal(buffer)
        end

        context 'with a compressor' do
          let :encoder do
            described_class.new(compressor)
          end

          let :compressor do
            double(:compressor)
          end

          let :request do
            PrepareRequest.new('SELECT * FROM things')
          end

          let :compressed_frame do
            encoder.encode_frame(request, 3).to_s
          end

          before do
            compressor.stub(:compress?).with("\x00\x00\x00\x14SELECT * FROM things").and_return(true)
            compressor.stub(:compress).with("\x00\x00\x00\x14SELECT * FROM things").and_return('FAKECOMPRESSEDBODY')
          end

          it 'sets the compression flag' do
            compressed_frame[1].should == "\x01"
          end

          it 'sets the size to the compressed size' do
            compressed_frame[4, 4].unpack('N').first.should == 18
          end

          it 'compresses the frame body' do
            compressed_frame[8, 18].should == 'FAKECOMPRESSEDBODY'
            compressed_frame.bytesize.should == 26
          end

          it 'does not clobber the trace flag' do
            request = PrepareRequest.new('SELECT * FROM things', true)
            stream_id = 3
            compressed_frame = encoder.encode_frame(request, stream_id)
            compressed_frame.to_s[1].should == "\x03"
          end

          it 'does not compress when the compressor responds to #compress? with false' do
            compressor.stub(:compress?).and_return(false)
            compressed_frame[1].should == "\x00"
            compressed_frame.should include('SELECT * FROM things')
            compressed_frame.bytesize.should == 32
          end
        end
      end

      describe '#change_stream_id' do
        it 'changes the stream ID byte' do
          buffer = ByteBuffer.new("\x01\x00\x03\x02\x00\x00\x00\x00")
          encoder.change_stream_id(99, buffer)
          buffer.discard(2)
          buffer.read_byte.should == 99
        end

        it 'changes the stream ID byte of the frame starting at the specified offset' do
          buffer = ByteBuffer.new("hello foo\x01\x00\x03\x02\x00\x00\x00\x00")
          encoder.change_stream_id(99, buffer, 9)
          buffer.discard(11)
          buffer.read_byte.should == 99
        end
      end
    end
  end
end