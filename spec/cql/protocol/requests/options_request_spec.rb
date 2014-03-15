# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe OptionsRequest do
      describe '#compressable?' do
        it 'is not compressable' do
          described_class.new.should_not be_compressable
        end
      end

      describe '#write' do
        it 'encodes an OPTIONS request frame (i.e. an empty body)' do
          bytes = OptionsRequest.new.write(1, CqlByteBuffer.new)
          bytes.should be_empty
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