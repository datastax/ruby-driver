# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe StartupRequest do
      describe '#initialize' do
        it 'raises an error if the CQL version is not specified' do
          expect { described_class.new(nil) }.to raise_error(ArgumentError)
        end
      end

      describe '#compressable?' do
        it 'is not compressable' do
          described_class.new('3.1.1').should_not be_compressable
        end
      end

      describe '#write' do
        it 'encodes a STARTUP request frame' do
          bytes = StartupRequest.new('3.0.0', 'snappy').write(1, CqlByteBuffer.new)
          bytes.should eql_bytes("\x00\x02\x00\x0bCQL_VERSION\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x06snappy")
        end

        it 'defaults to no compression' do
          bytes = StartupRequest.new('3.1.1').write(1, CqlByteBuffer.new)
          bytes.should eql_bytes("\x00\x01\x00\x0bCQL_VERSION\x00\x053.1.1")
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = StartupRequest.new('3.0.0')
          request.to_s.should == 'STARTUP {"CQL_VERSION"=>"3.0.0"}'
        end
      end
    end
  end
end