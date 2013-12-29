# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe ResponseFrame do
      let :frame do
        described_class.new
      end

      context 'when fed no data' do
        it 'has a header length' do
          frame.header_length.should == 8
        end

        it 'has no body length' do
          frame.body_length.should be_nil
        end

        it 'is not complete' do
          frame.should_not be_complete
        end
      end

      context 'when fed a header' do
        before do
          frame << "\x81\x00\x00\x02\x00\x00\x00\x16"
        end

        it 'knows the frame body length' do
          frame.body_length.should == 22
        end
      end

      context 'when fed a header in pieces' do
        before do
          frame << "\x81\x00"
          frame << "\x00\x02\x00\x00\x00"
          frame << "\x16"
        end

        it 'knows the body length' do
          frame.body_length.should == 22
        end

        it 'knows the stream ID' do
          frame.stream_id.should == 0
        end
      end

      context 'when fed a header with a non-zero stream ID' do
        before do
          frame << "\x81\x00\x20\x02\x00\x00\x00\x16"
        end

        it 'knows the stream ID' do
          frame.stream_id.should == 0x20
        end
      end

      context 'when fed a header with the stream ID -1' do
        before do
          frame << "\x81\x00\xff\x02\x00\x00\x00\x16"
        end

        it 'knows the stream ID' do
          frame.stream_id.should == -1
        end
      end

      context 'when fed a request frame header' do
        it 'raises an UnsupportedFrameTypeError' do
          expect { frame << "\x01\x00\x00\x00\x00\x00\x00\x00" }.to raise_error(UnsupportedFrameTypeError)
        end
      end

      context 'when fed a request frame header' do
        it 'raises an UnsupportedFrameTypeError' do
          expect { frame << "\x01\x00\x00\x00\x00\x00\x00\x00" }.to raise_error(UnsupportedFrameTypeError)
        end
      end

      context 'when fed a header and a partial body' do
        before do
          frame << "\x81\x00"
          frame << "\x00\x06"
          frame << "\x00\x00\x00\x16"
          frame << [rand(255), rand(255), rand(255), rand(255), rand(255), rand(255), rand(255), rand(255)].pack('C')
        end

        it 'knows the body length' do
          frame.body_length.should == 22
        end

        it 'is not complete' do
          frame.should_not be_complete
        end
      end

      context 'when fed a compressed frame' do
        let :compressor do
          double(:compressor)
        end

        it 'decompresses the body' do
          compressor.stub(:decompress).with('FAKECOMPRESSEDBODY').and_return("\x00\x00\x00\x63\x00\x05Bork!")
          frame = described_class.new(nil, compressor)
          frame << "\x81\x01\x00\x00\x00\x00\x00\x12FAKECOMPRESSEDBODY"
          frame.body.code.should == 99
          frame.body.message.should == 'Bork!'
        end

        it 'ignores extra bytes in the compressed body' do
          compressor.stub(:decompress).with('FAKECOMPRESSEDBODY').and_return("\x00\x00\x00\x63\x00\x05Bork!HELLOWORLD")
          frame = described_class.new(nil, compressor)
          frame << "\x81\x01\x00\x00\x00\x00\x00\x12FAKECOMPRESSEDBODY"
          frame.body.code.should == 99
          frame.body.message.should == 'Bork!'
        end

        it 'extracts the trace ID' do
          compressor.stub(:decompress).with('FAKECOMPRESSEDBODY').and_return("\a\xE4\xBE\x10?\x03\x11\xE3\x951\xFBr\xEF\xF0_\xBB\x00\x00\x00\x01")
          frame = described_class.new(nil, compressor)
          frame << "\x81\x03\x00\x08\x00\x00\x00\x12FAKECOMPRESSEDBODY"
          frame.body.trace_id.should == Uuid.new('07e4be10-3f03-11e3-9531-fb72eff05fbb')
        end

        it 'raises an error when the frame is compressed but no compressor is specified' do
          expect { frame << "\x81\x01\x00\x00\x00\x00\x00\x12FAKECOMPRESSEDBODY" }.to raise_error(UnexpectedCompressionError)
        end
      end

      context 'when the tracing flag is set' do
        let :frame_bytes do
          "\x81\x02\x00\b\x00\x00\x00U\a\xE4\xBE\x10?\x03\x11\xE3\x951\xFBr\xEF\xF0_\xBB\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\ncql_rb_602\x00\x05users\x00\tuser_name\x00\r\x00\x05email\x00\r\x00\bpassword\x00\r\x00\x00\x00\x00"
        end

        it 'decodes the frame' do
          frame = described_class.new
          frame << frame_bytes
          frame.body.rows
        end

        it 'extracts the trace ID' do
          frame = described_class.new
          frame << frame_bytes
          frame.body.trace_id.should == Uuid.new('07e4be10-3f03-11e3-9531-fb72eff05fbb')
        end
      end

      context 'when fed an non-existent opcode' do
        it 'raises an UnsupportedOperationError' do
          expect { frame << "\x81\x00\x00\x99\x00\x00\x00\x02\x11\x22" }.to raise_error(UnsupportedOperationError)
        end
      end

      context 'when fed more bytes than needed' do
        it 'it consumes its bytes, leaving the rest' do
          buffer = ByteBuffer.new("\x81\x00\x00\x06\x00\x00\x00\x27\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00")
          buffer << "\x81\x00\x00\x00"
          described_class.new(buffer)
          buffer.should eql_bytes("\x81\x00\x00\x00")
        end
      end

      context 'when fed a frame that is longer than the specification specifies' do
        it 'it consumes the body length, leaving the rest' do
          buffer = ByteBuffer.new("\x81\x00\x00\x06\x00\x00\x00\x2a")
          buffer << "\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00"
          buffer << "\xab\xcd\xef\x99"
          described_class.new(buffer)
          buffer.should eql_bytes("\x99")
        end
      end
    end
  end
end