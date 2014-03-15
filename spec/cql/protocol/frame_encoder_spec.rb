# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe FrameEncoder do
      describe '#encode_frame' do
        let :request do
          double(:request)
        end

        let :compressor do
          double(:compressor)
        end

        before do
          request.stub(:opcode).and_return(0x77)
          request.stub(:trace?).and_return(false)
          request.stub(:write) { |pv, bb| bb }
        end

        it 'asks the request to write itself to a buffer' do
          encoder = described_class.new(1)
          encoder.encode_frame(request)
          request.should have_received(:write).with(anything, an_instance_of(CqlByteBuffer))
        end

        it 'appends what the request wrote to the specified buffer, after a header' do
          buffer = CqlByteBuffer.new('hello')
          request.stub(:write) { |pv, bb| bb << "\x01\x02\x03" }
          encoder = described_class.new(1)
          encoder.encode_frame(request, 0, buffer)
          buffer.to_s[ 0, 5].should == 'hello'
          buffer.to_s[13, 3].should == "\x01\x02\x03"
        end

        it 'returns the specified buffer' do
          buffer = CqlByteBuffer.new('hello')
          encoder = described_class.new(1)
          returned_buffer = encoder.encode_frame(request, 0, buffer)
          returned_buffer.should equal(buffer)
        end

        it 'passes the protocol version to the request' do
          encoder = described_class.new(7)
          encoder.encode_frame(request)
          request.should have_received(:write).with(7, anything)
        end

        it 'encodes a header with the specified protocol version' do
          encoder = described_class.new(7)
          buffer = encoder.encode_frame(request)
          buffer.to_s[0].should == "\x07"
        end

        it 'encodes a header with the tracing flag set' do
          request.stub(:trace?).and_return(true)
          encoder = described_class.new(1)
          buffer = encoder.encode_frame(request)
          buffer.to_s[1].should == "\x02"
        end

        it 'encodes a header with the specified stream ID' do
          encoder = described_class.new(1)
          buffer = encoder.encode_frame(request, 9)
          buffer.to_s[2].should == "\x09"
        end

        it 'complains when the stream ID is less than 0 or more than 127' do
          encoder = described_class.new(1)
          expect { encoder.encode_frame(request, -1) }.to raise_error(InvalidStreamIdError)
          expect { encoder.encode_frame(request, 128) }.to raise_error(InvalidStreamIdError)
        end

        it 'encodes a header with the right opcode' do
          encoder = described_class.new(1)
          buffer = encoder.encode_frame(request, 9)
          buffer.to_s[3].should == "\x77"
        end

        it 'encodes the body size' do
          request.stub(:write).and_return(CqlByteBuffer.new('helloworld'))
          encoder = described_class.new(1)
          buffer = encoder.encode_frame(request, 9)
          buffer.to_s[4, 4].should == "\x00\x00\x00\x0a"
        end

        context 'when a compressor has been specified' do
          before do
            request.stub(:compressable?).and_return(true)
            request.stub(:write).and_return(CqlByteBuffer.new('helloworld'))
            compressor.stub(:compress?).and_return(true)
            compressor.stub(:compress).with('helloworld').and_return('COMPRESSEDFRAME')
          end

          it 'compresses the request' do
            encoder = described_class.new(1, compressor)
            buffer = encoder.encode_frame(request)
            buffer.to_s[8, 100].should == 'COMPRESSEDFRAME'
          end

          it 'sets the compression flag' do
            encoder = described_class.new(1, compressor)
            buffer = encoder.encode_frame(request)
            buffer.to_s[1, 1].should == "\x01"
          end

          it 'sets both the compression flag and the tracing flag' do
            request.stub(:trace?).and_return(true)
            encoder = described_class.new(1, compressor)
            buffer = encoder.encode_frame(request)
            buffer.to_s[1, 1].should == "\x03"
          end

          it 'encodes the compressed body size' do
            encoder = described_class.new(1, compressor)
            buffer = encoder.encode_frame(request)
            buffer.to_s[4, 4].should == "\x00\x00\x00\x0f"
          end

          it 'does not compress uncompressable frames' do
            request.stub(:compressable?).and_return(false)
            encoder = described_class.new(1, compressor)
            buffer = encoder.encode_frame(request)
            compressor.should_not have_received(:compress?)
            compressor.should_not have_received(:compress)
          end
        end
      end

      describe '#change_stream_id' do
        let :encoder do
          described_class.new
        end

        it 'changes the stream ID byte' do
          buffer = CqlByteBuffer.new("\x01\x00\x03\x02\x00\x00\x00\x00")
          encoder.change_stream_id(99, buffer)
          buffer.discard(2)
          buffer.read_byte.should == 99
        end

        it 'changes the stream ID byte of the frame starting at the specified offset' do
          buffer = CqlByteBuffer.new("hello foo\x01\x00\x03\x02\x00\x00\x00\x00")
          encoder.change_stream_id(99, buffer, 9)
          buffer.discard(11)
          buffer.read_byte.should == 99
        end
      end
    end
  end
end