# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe RegisterRequest do
      describe '#encode_frame' do
        it 'encodes a REGISTER request frame' do
          bytes = RegisterRequest.new('TOPOLOGY_CHANGE', 'STATUS_CHANGE').encode_frame(3)
          bytes.should == "\x01\x00\x03\x0b\x00\x00\x00\x22\x00\x02\x00\x0fTOPOLOGY_CHANGE\x00\x0dSTATUS_CHANGE"
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = RegisterRequest.new('TOPOLOGY_CHANGE', 'STATUS_CHANGE')
          request.to_s.should == 'REGISTER ["TOPOLOGY_CHANGE", "STATUS_CHANGE"]'
        end
      end
    end
  end
end
