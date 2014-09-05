# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require 'spec_helper'


shared_examples 'compressor' do |algorithm, compressed_string|
  describe '#algorithm' do
    it %(returns "#{algorithm}") do
      described_class.new.algorithm.should == algorithm
    end
  end

  describe '#compress?' do
    it 'returns true for arguments larger than 64 bytes' do
      described_class.new.compress?('x' * 65).should be_truthy
    end

    it 'returns false for arguments smaller than 64 bytes' do
      described_class.new.compress?('x' * 64).should be_falsey
    end

    it 'is customizable via a constructor argument' do
      described_class.new(89).compress?('x' * 90).should be_truthy
      described_class.new(89).compress?('x' * 89).should be_falsey
    end
  end

  describe '#compress/#decompress' do
    let :compressor do
      described_class.new
    end

    it 'compresses strings' do
      input = 'hello' * 100
      compressed = compressor.compress(input)
      compressed.bytesize.should be < input.bytesize
    end

    it 'compresses byte buffers' do
      input = Cassandra::Protocol::CqlByteBuffer.new('hello' * 100)
      compressed = compressor.compress(input)
      compressed.should == compressor.compress(input.to_s)
    end

    it 'decompresses compressed strings' do
      input = compressed_string
      decompressed = compressor.decompress(input)
      decompressed.should == 'hellohellohellohellohello'
    end

    it 'decompresses byte buffers' do
      input = Cassandra::Protocol::CqlByteBuffer.new(compressed_string)
      decompressed = compressor.decompress(input)
      decompressed.should == 'hellohellohellohellohello'
    end

    it 'decompresses its own compressed output' do
      input = 'ƒ∂' * 100
      output = compressor.decompress(compressor.compress(input))
      output.force_encoding(::Encoding::UTF_8)
      output.should == input
    end

    it 'returns binary strings' do
      compressed = compressor.compress('hello' * 100)
      decompressed = compressor.decompress(compressed)
      compressed.encoding.should == ::Encoding::BINARY
      decompressed.encoding.should == ::Encoding::BINARY
    end
  end
end
