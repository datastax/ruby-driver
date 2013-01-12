# encoding: ascii-8bit

require 'spec_helper'


module Cql
  describe Encoding do
    let :buffer do
      ''
    end

    describe '#write_int' do
    end

    describe '#write_short' do
    end

    describe '#write_string' do
      it 'encodes a string' do
        Encoding.write_string(buffer, 'hello')
        buffer.should == "\x00\x05hello"
      end

      it 'encodes an empty string' do
        Encoding.write_string(buffer, '')
        buffer.should == "\x00\x00"
      end

      it 'returns the buffer' do
        result = Encoding.write_string(buffer, 'hello')
        result.should equal(buffer)
      end
    end

    describe '#write_long_string' do
    end

    describe '#write_uuid' do
    end

    describe '#write_string_list' do
    end

    describe '#write_bytes' do
    end

    describe '#write_short_bytes' do
    end

    describe '#write_option' do
    end

    describe '#write_option_list' do
    end

    describe '#write_inet' do
    end

    describe '#write_consistency' do
    end

    describe '#write_string_map' do
      it 'encodes a string map' do
        Encoding.write_string_map(buffer, 'HELLO' => 'world', 'foo' => 'bar')
        buffer.should == "\x00\x02\x00\x05HELLO\x00\x05world\x00\x03foo\x00\x03bar"
      end

      it 'encodes an empty map' do
        Encoding.write_string_map(buffer, {})
        buffer.should == "\x00\x00"
      end

      it 'returns the buffer' do
        result = Encoding.write_string_map(buffer, 'HELLO' => 'world')
        result.should equal(buffer)
      end
    end

    describe '#write_string_multimap' do
    end
  end
end