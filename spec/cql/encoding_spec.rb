# encoding: ascii-8bit

require 'spec_helper'


module Cql
  describe Encoding do
    let :buffer do
      ''
    end

    describe '#write_int' do
      it 'encodes an int'
      it 'returns the buffer'
    end

    describe '#write_short' do
      it 'encodes a short'
      it 'returns the buffer'
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
      it 'encodes a string'
      it 'returns the buffer'
    end

    describe '#write_uuid' do
      it 'encodes an UUID'
      it 'returns the buffer'
    end

    describe '#write_string_list' do
      it 'encodes a string list' do
        Encoding.write_string_list(buffer, %w[foo bar hello world])
        buffer.should == "\x00\x04\x00\x03foo\x00\x03bar\x00\x05hello\x00\x05world"
      end

      it 'encodes an empty string list' do
        Encoding.write_string_list(buffer, [])
        buffer.should == "\x00\x00"
      end

      it 'returns the buffer' do
        result = Encoding.write_string_list(buffer, %w[foo])
        result.should equal(buffer)
      end
    end

    describe '#write_bytes' do
      it 'encodes a byte array'
      it 'returns the buffer'
    end

    describe '#write_short_bytes' do
      it 'encodes a byte array'
      it 'returns the buffer'
    end

    describe '#write_option' do
      it 'encodes an option'
      it 'returns the buffer'
    end

    describe '#write_option_list' do
      it 'encodes an option list'
      it 'returns the buffer'
    end

    describe '#write_inet' do
      it 'encodes an IPv4 + port pair'
      it 'encodes an IPv6 + port pair'
      it 'returns the buffer'
    end

    describe '#write_consistency' do
      it 'encodes a consistency symbol'
      it 'returns the buffer'
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
      it 'encodes a string multimap'
      it 'returns the buffer'
    end
  end
end