# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe SupportedResponse do
      describe '.decode' do
        let :response do
          buffer = CqlByteBuffer.new("\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00")
          described_class.decode(1, buffer, buffer.length)
        end

        it 'decodes the options' do
          response.options.should == {'CQL_VERSION' => ['3.0.0'], 'COMPRESSION' => []}
        end
      end

      describe '#to_s' do
        it 'returns a string with the options' do
          response = described_class.new('CQL_VERSION' => ['3.0.0'], 'COMPRESSION' => [])
          response.to_s.should == 'SUPPORTED {"CQL_VERSION"=>["3.0.0"], "COMPRESSION"=>[]}'
        end
      end
    end
  end
end
