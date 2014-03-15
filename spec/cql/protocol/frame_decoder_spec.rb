# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe FrameDecoder do
      let :decoder do
        described_class.new
      end

      describe '#decode_frame' do
        context 'with an uncompressed frame' do
          it 'returns a partial frame when not all the frame\'s bytes are in the buffer' do
            frame = decoder.decode_frame(CqlByteBuffer.new)
            frame.should_not be_complete
            frame = decoder.decode_frame(CqlByteBuffer.new("\x81\x00\x00\x02\x00\x00\x00\x16"))
            frame.should_not be_complete
            frame = decoder.decode_frame(CqlByteBuffer.new("\x81\x00\x00\x02\x00\x00\x00\x16\x01\x23\x45"))
            frame.should_not be_complete
          end

          it 'returns a partial frame less than eight bytes are missing' do
            frame = decoder.decode_frame(CqlByteBuffer.new("\x81\x02\x00\x08\x00\x00\x00\x14\a\xE4\xBE\x10?\x03\x11\xE3\x951\xFBr\xEF\xF0_\xBB\x00\x00"))
            frame.should_not be_complete
          end

          it 'returns a complete frame when all bytes are in the buffer' do
            buffer = CqlByteBuffer.new
            buffer << "\x81\x00\x00\x06\x00\x00\x00\x27"
            buffer << "\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00"
            frame = decoder.decode_frame(buffer)
            frame.should be_complete
            frame.body.should be_a(Response)
          end

          it 'returns the stream ID' do
            buffer = CqlByteBuffer.new("\x81\x00\x03\x02\x00\x00\x00\x00")
            frame = decoder.decode_frame(buffer)
            frame.stream_id.should == 3
          end

          it 'returns the stream ID when it is negative' do
            buffer = CqlByteBuffer.new("\x81\x00\xff\x02\x00\x00\x00\x00")
            frame = decoder.decode_frame(buffer)
            frame.stream_id.should == -1
          end

          it 'first returns a partial frame, then a complete frame' do
            buffer = CqlByteBuffer.new
            buffer << "\x81\x00\x00\x06\x00\x00\x00\x27"
            frame = decoder.decode_frame(buffer)
            buffer << "\x00\x02\x00\x0bCQL_VERSION"
            frame = decoder.decode_frame(buffer, frame)
            buffer << "\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00"
            frame = decoder.decode_frame(buffer, frame)
            frame.should be_complete
          end

          it 'leaves extra bytes in the buffer' do
            buffer = CqlByteBuffer.new
            buffer << "\x81\x00\x00\x06\x00\x00\x00\x27"
            buffer << "\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00\xca\xfe"
            frame = decoder.decode_frame(buffer)
            frame.should be_complete
            buffer.to_s.should == "\xca\xfe"
          end

          it 'consumes the number of bytes specified in the frame header' do
            buffer = CqlByteBuffer.new
            buffer << "\x81\x00\x00\x06\x00\x00\x00\x29"
            buffer << "\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00\xca\xfe"
            frame = decoder.decode_frame(buffer)
            frame.should be_complete
            buffer.to_s.should be_empty
          end

          it 'extracts a trace ID' do
            buffer = CqlByteBuffer.new
            buffer << "\x81\x02\x00\x08\x00\x00\x00\x14\a\xE4\xBE\x10?\x03\x11\xE3\x951\xFBr\xEF\xF0_\xBB\x00\x00\x00\x01"
            frame = decoder.decode_frame(buffer)
            frame.body.trace_id.should == Uuid.new('07e4be10-3f03-11e3-9531-fb72eff05fbb')
          end

          it 'complains when the frame is a request frame' do
            expect { decoder.decode_frame(CqlByteBuffer.new("\x01\x00\x00\x05\x00\x00\x00\x00")) }.to raise_error(UnsupportedFrameTypeError)
          end

          it 'complains when the opcode is unknown' do
            expect { decoder.decode_frame(CqlByteBuffer.new("\x81\x00\x00\xff\x00\x00\x00\x00")) }.to raise_error(UnsupportedOperationError)
          end
        end

        context 'with a compressed frame' do
          let :decoder do
            described_class.new(compressor)
          end

          let :compressor do
            double(:compressor)
          end

          let :buffer do
            CqlByteBuffer.new("\x81\x01\x00\x06\x00\x00\x00\x12FAKECOMPRESSEDBODY")
          end

          before do
            compressor.stub(:decompress).with('FAKECOMPRESSEDBODY').and_return("\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00")
          end

          it 'decompresses the body bytes' do
            frame = decoder.decode_frame(buffer)
            frame.body.should be_a(Response)
          end

          it 'leaves extra bytes in the buffer' do
            buffer << 'EXTRABYTES'
            frame = decoder.decode_frame(buffer)
            buffer.to_s.should == 'EXTRABYTES'
          end

          it 'extracts a trace ID' do
            buffer.update(1, "\x03\x00\x08")
            compressor.stub(:decompress).with('FAKECOMPRESSEDBODY').and_return("\a\xE4\xBE\x10?\x03\x11\xE3\x951\xFBr\xEF\xF0_\xBB\x00\x00\x00\x01")
            frame = decoder.decode_frame(buffer)
            frame.body.trace_id.should == Uuid.new('07e4be10-3f03-11e3-9531-fb72eff05fbb')
          end

          it 'complains when there is no compressor' do
            expect { described_class.new.decode_frame(buffer) }.to raise_error(UnexpectedCompressionError)
          end
        end
      end
    end
  end
end