# encoding: utf-8

require 'spec_helper'


shared_examples 'compressor' do |algorithm|
  describe '#algorithm' do
    it %(returns "#{algorithm}") do
      described_class.new.algorithm.should == algorithm
    end
  end

  describe '#compress?' do
    it 'returns true for arguments larger than 64 bytes' do
      described_class.new.compress?('x' * 65).should be_true
    end

    it 'returns false for arguments smaller than 64 bytes' do
      described_class.new.compress?('x' * 64).should be_false
    end

    it 'is customizable via a constructor argument' do
      described_class.new(89).compress?('x' * 90).should be_true
      described_class.new(89).compress?('x' * 89).should be_false
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

    it 'decompresses compressed strings' do
      input = "\x19\x10helloN\x05\x00"
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
