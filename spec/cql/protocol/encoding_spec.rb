# encoding: utf-8

require 'spec_helper'


module Cql
  module Protocol
    describe Encoding do
      let :buffer do
        ByteBuffer.new
      end

      describe '#write_int' do
        it 'encodes an int' do
          Encoding.write_int(buffer, 2323234234)
          buffer.should eql_bytes("\x8a\x79\xbd\xba")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_int(buffer, 10)
          buffer.should eql_bytes("\xab\x00\x00\x00\x0a")
        end

        it 'returns the buffer' do
          result = Encoding.write_int(buffer, 2323234234)
          result.should equal(buffer)
        end
      end

      describe '#write_short' do
        it 'encodes a short' do
          Encoding.write_short(buffer, 0xabcd)
          buffer.should eql_bytes("\xab\xcd")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_short(buffer, 10)
          buffer.should eql_bytes("\xab\x00\x0a")
        end

        it 'returns the buffer' do
          result = Encoding.write_short(buffer, 42)
          result.should equal(buffer)
        end
      end

      describe '#write_string' do
        it 'encodes a string' do
          Encoding.write_string(buffer, 'hello')
          buffer.should eql_bytes("\x00\x05hello")
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = 'I love π'
          Encoding.write_string(buffer, str)
          buffer.should eql_bytes("\xff\x00\x09I love π")
        end

        it 'encodes an empty string' do
          Encoding.write_string(buffer, '')
          buffer.should eql_bytes("\x00\x00")
        end

        it 'encodes a non-string' do
          Encoding.write_string(buffer, 42)
          buffer.should eql_bytes("\x00\x0242")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_string(buffer, 'foo')
          buffer.should eql_bytes("\xab\x00\x03foo")
        end

        it 'returns the buffer' do
          result = Encoding.write_string(buffer, 'hello')
          result.should equal(buffer)
        end
      end

      describe '#write_long_string' do
        it 'encodes a string' do
          Encoding.write_long_string(buffer, 'hello world ' * 100_000)
          buffer.read(45).should eql_bytes("\x00\x12\x4f\x80hello world hello world hello world hello")
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = 'I love π'
          Encoding.write_long_string(buffer, str)
          buffer.should eql_bytes("\xff\x00\x00\x00\x09I love π")
        end

        it 'encodes an empty string' do
          Encoding.write_long_string(buffer, '')
          buffer.should eql_bytes("\x00\x00\x00\x00")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_long_string(buffer, 'foo')
          buffer.should eql_bytes("\xab\x00\x00\x00\x03foo")
        end

        it 'returns the buffer' do
          result = Encoding.write_long_string(buffer, 'hello')
          result.should equal(buffer)
        end
      end

      describe '#write_uuid' do
        let :uuid do
          Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        end

        it 'encodes an UUID' do
          Encoding.write_uuid(buffer, uuid)
          buffer.should eql_bytes("\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11")
        end

        it 'encodes a UUID as 16 bytes' do
          Encoding.write_uuid(buffer, Uuid.new('00000000-24e1-11df-8924-001ff3591711'))
          buffer.size.should eql(16)
        end

        it 'appends to the buffer' do
          buffer << 'FOO'
          Encoding.write_uuid(buffer, uuid)
          buffer.read(3).should eql_bytes('FOO')
        end

        it 'returns the buffer' do
          result = Encoding.write_uuid(buffer, uuid)
          result.should equal(buffer)
        end
      end

      describe '#write_string_list' do
        it 'encodes a string list' do
          Encoding.write_string_list(buffer, %w[foo bar hello world])
          buffer.should eql_bytes("\x00\x04\x00\x03foo\x00\x03bar\x00\x05hello\x00\x05world")
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = %w[I love π]
          Encoding.write_string_list(buffer, str)
          buffer.should eql_bytes("\xff\x00\x03\x00\x01I\x00\x04love\x00\x02π")
        end

        it 'encodes an empty string list' do
          Encoding.write_string_list(buffer, [])
          buffer.should eql_bytes("\x00\x00")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_string_list(buffer, %w[foo bar])
          buffer.should eql_bytes("\xab\x00\x02\x00\x03foo\x00\x03bar")
        end

        it 'returns the buffer' do
          result = Encoding.write_string_list(buffer, %w[foo])
          result.should equal(buffer)
        end
      end

      describe '#write_bytes' do
        it 'encodes a byte array' do
          Encoding.write_bytes(buffer, "\xaa" * 2000)
          buffer.should eql_bytes("\x00\x00\x07\xd0" << ("\xaa" * 2000))
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = 'I love π'
          Encoding.write_bytes(buffer, str)
          buffer.should eql_bytes("\xff\x00\x00\x00\x09I love π")
        end

        it 'encodes nil' do
          Encoding.write_bytes(buffer, nil)
          buffer.should eql_bytes("\xff\xff\xff\xff")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_bytes(buffer, "\xf0\x0b\xbar")
          buffer.should eql_bytes("\xab\x00\x00\x00\x04\xf0\x0b\xbar")
        end

        it 'returns the buffer' do
          result = Encoding.write_bytes(buffer, "\xab")
          result.should equal(buffer)
        end
      end

      describe '#write_short_bytes' do
        it 'encodes a byte array' do
          Encoding.write_short_bytes(buffer, "\xaa\xbb\xcc")
          buffer.should eql_bytes("\x00\x03\xaa\xbb\xcc")
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = 'I love π'
          Encoding.write_short_bytes(buffer, str)
          buffer.should eql_bytes("\xff\x00\x09I love π")
        end

        it 'encodes nil' do
          Encoding.write_short_bytes(buffer, nil)
          buffer.should eql_bytes("\xff\xff")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_short_bytes(buffer, "\xf0\x0b\xbar")
          buffer.should eql_bytes("\xab\x00\x04\xf0\x0b\xbar")
        end

        it 'returns the buffer' do
          result = Encoding.write_short_bytes(buffer, "\xab")
          result.should equal(buffer)
        end
      end

      describe '#write_consistency' do
        {
          :any => "\x00\x00",
          :one => "\x00\x01",
          :two => "\x00\x02",
          :three => "\x00\x03",
          :quorum => "\x00\x04",
          :all => "\x00\x05",
          :local_quorum => "\x00\x06",
          :each_quorum => "\x00\x07",
          :serial => "\x00\x08",
          :local_serial => "\x00\x09",
          :local_one => "\x00\x0a",
        }.each do |consistency, expected_encoding|
          it "encodes #{consistency}" do
            Encoding.write_consistency(buffer, consistency)
            buffer.should eql_bytes(expected_encoding)
          end
        end

        it 'raises an exception for an unknown consistency' do
          expect { Encoding.write_consistency(buffer, :foo) }.to raise_error(EncodingError)
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_consistency(buffer, :one)
          buffer.should eql_bytes("\xab\x00\x01")
        end

        it 'returns the buffer' do
          result = Encoding.write_consistency(buffer, :quorum)
          result.should equal(buffer)
        end
      end

      describe '#write_string_map' do
        it 'encodes a string map' do
          Encoding.write_string_map(buffer, 'HELLO' => 'world', 'foo' => 'bar')
          buffer.should eql_bytes("\x00\x02\x00\x05HELLO\x00\x05world\x00\x03foo\x00\x03bar")
        end

        it 'encodes an empty map' do
          Encoding.write_string_map(buffer, {})
          buffer.should eql_bytes("\x00\x00")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          Encoding.write_string_map(buffer, 'foo' => 'bar')
          buffer.should eql_bytes("\xab\x00\x01\x00\x03foo\x00\x03bar")
        end

        it 'returns the buffer' do
          result = Encoding.write_string_map(buffer, 'HELLO' => 'world')
          result.should equal(buffer)
        end
      end

      describe '#write_long' do
        it 'encodes a long' do
          Encoding.write_long(buffer, 0x0123456789)
          buffer.should eql_bytes("\x00\x00\x00\x01\x23\x45\x67\x89")
        end

        it 'appends to the buffer' do
          buffer << "\x99"
          Encoding.write_long(buffer, 0x0123456789)
          buffer.should eql_bytes("\x99\x00\x00\x00\x01\x23\x45\x67\x89")
        end

        it 'returns the buffer' do
          result = Encoding.write_long(buffer, 1)
          result.should equal(buffer)
        end
      end

      describe '#write_varint' do
        it 'encodes a variable length integer' do
          Encoding.write_varint(buffer, 1231312312331283012830129382342342412123)
          buffer.should eql_bytes("\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[")
        end

        it 'encodes a negative variable length integer' do
          Encoding.write_varint(buffer, -234234234234)
          buffer.should eql_bytes("\xC9v\x8D:\x86")
        end

        it 'appends to the buffer' do
          buffer << "\x99"
          Encoding.write_varint(buffer, -234234234234)
          buffer.should eql_bytes("\x99\xC9v\x8D:\x86")
        end

        it 'returns the buffer' do
          result = Encoding.write_varint(buffer, -234234234234)
          result.should equal(buffer)
        end
      end

      describe '#write_decimal' do
        it 'encodes a BigDecimal as a decimal' do
          Encoding.write_decimal(buffer, BigDecimal.new('1042342234234.123423435647768234'))
          buffer.should eql_bytes("\x00\x00\x00\x12\r'\xFDI\xAD\x80f\x11g\xDCfV\xAA")
        end

        it 'appends to the buffer' do
          buffer << "\x99"
          Encoding.write_decimal(buffer, BigDecimal.new('1042342234234.123423435647768234'))
          buffer.read(1).should eql_bytes("\x99")
        end

        it 'returns the buffer' do
          result = Encoding.write_decimal(buffer, BigDecimal.new('3.14'))
          result.should equal(buffer)
        end
      end

      describe '#write_double' do
        it 'encodes a double' do
          Encoding.write_double(buffer, 10000.123123123)
          buffer.should eql_bytes("@\xC3\x88\x0F\xC2\x7F\x9DU")
        end

        it 'appends to the buffer' do
          buffer << 'BEFORE'
          Encoding.write_double(buffer, 10000.123123123)
          buffer.read(6).should eql_bytes('BEFORE')
        end

        it 'returns the buffer' do
          result = Encoding.write_double(buffer, 10000.123123123)
          result.should equal(buffer)
        end
      end

      describe '#write_float' do
        it 'encodes a float' do
          Encoding.write_float(buffer, 12.13)
          buffer.should eql_bytes("AB\x14{")
        end

        it 'appends to the buffer' do
          buffer << 'BEFORE'
          Encoding.write_float(buffer, 12.13)
          buffer.read(6).should eql_bytes('BEFORE')
        end

        it 'returns the buffer' do
          result = Encoding.write_float(buffer, 12.13)
          result.should equal(buffer)
        end
      end
    end
  end
end