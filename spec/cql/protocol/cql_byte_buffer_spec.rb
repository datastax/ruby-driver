# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe CqlByteBuffer do
      let :buffer do
        described_class.new
      end
      
      describe '#read_unsigned_byte' do
        let :buffer do
          described_class.new("\xab")
        end

        it 'decodes a raw byte' do
          buffer.read_unsigned_byte.should == 0xab
        end

        it 'consumes the byte' do
          buffer.read_unsigned_byte
          buffer.should be_empty
        end

        it 'raises an error when there is no byte available' do
          expect { described_class.new.read_unsigned_byte }.to raise_error(DecodingError)
        end
      end

      describe '#read_varint' do
        it 'decodes a variable length integer' do
          buffer = described_class.new("\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[")
          buffer.read_varint(17).should == 1231312312331283012830129382342342412123
        end

        it 'decodes a negative variable length integer' do
          buffer = described_class.new("\xC9v\x8D:\x86")
          buffer.read_varint(5).should == -234234234234
        end

        it 'decodes an unsigned variable length integer' do
          buffer = described_class.new("\xC9v\x8D:\x86")
          buffer.read_varint(5, false).should == 865277393542
        end

        it 'consumes the bytes' do
          buffer = described_class.new("\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[\x01\x02\x03")
          buffer.read_varint(17)
          buffer.should eql_bytes("\x01\x02\x03")
        end

        it 'raises an error when there is not enough bytes available' do
          buffer = described_class.new("\xC9v\x8D:")
          expect { buffer.read_varint(7) }.to raise_error(DecodingError)
        end
      end

      describe '#read_decimal' do
        let :buffer do
          described_class.new("\x00\x00\x00\x12\r'\xFDI\xAD\x80f\x11g\xDCfV\xAA")
        end

        it 'decodes a decimal to a BigDecimal' do
          buffer.read_decimal.should == BigDecimal.new('1042342234234.123423435647768234')
        end

        it 'decodes a negative decimal' do
          buffer = described_class.new("\x00\x00\x00\x12\xF2\xD8\x02\xB6R\x7F\x99\xEE\x98#\x99\xA9V")
          buffer.read_decimal.should == BigDecimal.new('-1042342234234.123423435647768234')
        end

        it 'decodes a positive decimal with only fractions' do
          buffer = described_class.new("\x00\x00\x00\x13*\xF8\xC4\xDF\xEB]o")
          buffer.read_decimal.should == BigDecimal.new('0.0012095473475870063')
        end

        it 'decodes a negative decimal with only fractions' do
          buffer = described_class.new("\x00\x00\x00\x13\xD5\a;\x20\x14\xA2\x91")
          buffer.read_decimal.should == BigDecimal.new('-0.0012095473475870063')
        end

        it 'consumes the bytes' do
          buffer << 'HELLO'
          buffer.read_decimal(buffer.length - 5)
          buffer.should eql_bytes('HELLO')
        end

        it 'defaults to using the buffer length' do
          b1 = buffer
          b2 = buffer.dup
          b1.read_decimal.should == b2.read_decimal(b2.length)
        end

        it 'raises an error when there is not enough bytes available' do
          b = described_class.new(buffer.read(3))
          expect { b.read_decimal(7) }.to raise_error(DecodingError)
        end
      end

      describe '#read_long' do
        it 'decodes a positive long' do
          buffer = described_class.new("\x00\x00\xca\xfe\xba\xbe\x00\x00")
          buffer.read_long.should == 0x0000cafebabe0000
        end

        it 'decodes a negative long' do
          buffer = described_class.new("\xff\xee\xdd\xcc\xbb\xaa\x99\x88")
          buffer.read_long.should == 0xffeeddccbbaa9988 - 0x10000000000000000
        end

        it 'consumes the bytes' do
          buffer = described_class.new("\xca\xfe\xba\xbe\xca\xfe\xba\xbe\xca\xfe\xba\xbe")
          buffer.read_long
          buffer.should eql_bytes("\xca\xfe\xba\xbe")
        end

        it 'raises an error when there is not enough bytes available' do
          b = described_class.new("\xca\xfe\xba\xbe\x00")
          expect { b.read_long }.to raise_error(DecodingError)
        end
      end

      describe '#read_double' do
        it 'decodes a double' do
          buffer = described_class.new("@\xC3\x88\x0F\xC2\x7F\x9DU")
          buffer.read_double.should == 10000.123123123
        end

        it 'consumes the bytes' do
          buffer = described_class.new("@\xC3\x88\x0F\xC2\x7F\x9DUxyz")
          buffer.read_double
          buffer.should eql_bytes('xyz')
        end

        it 'raises an error when there is not enough bytes available' do
          buffer = described_class.new("@\xC3\x88\x0F")
          expect { buffer.read_double }.to raise_error(DecodingError)
        end
      end

      describe '#read_float' do
        it 'decodes a float' do
          buffer = described_class.new("AB\x14{")
          buffer.read_float.should be_within(0.00001).of(12.13)
        end

        it 'consumes the bytes' do
          buffer = described_class.new("AB\x14{xyz")
          buffer.read_float
          buffer.should eql_bytes('xyz')
        end

        it 'raises an error when there is not enough bytes available' do
          buffer = described_class.new("\x0F")
          expect { buffer.read_float }.to raise_error(DecodingError)
        end
      end

      describe '#read_signed_int' do
        let :buffer do
          described_class.new("\x00\xff\x00\xff")
        end

        it 'decodes a positive int' do
          buffer.read_signed_int.should == 0x00ff00ff
        end

        it 'decodes a negative int' do
          buffer = described_class.new("\xff\xee\xdd\xcc")
          buffer.read_signed_int.should == 0xffeeddcc - 0x100000000
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          buffer.read_signed_int
          buffer.should eql_bytes("\xab\xcd")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          buffer = described_class.new("\x01\xab")
          expect { buffer.read_signed_int }.to raise_error(DecodingError)
        end
      end

      describe '#read_unsigned_short' do
        let :buffer do
          described_class.new("\x00\x02")
        end

        it 'decodes a short' do
          buffer.read_unsigned_short.should == 2
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          buffer.read_unsigned_short
          buffer.should eql_bytes("\xff\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          buffer = described_class.new("\x01")
          expect { buffer.read_unsigned_short }.to raise_error(DecodingError)
        end
      end

      describe '#read_string' do
        let :buffer do
          described_class.new("\x00\x0bhej och hå")
        end

        it 'decodes a string' do
          buffer.read_string.should == 'hej och hå'.force_encoding(::Encoding::UTF_8)
        end

        it 'decodes a string as UTF-8' do
          buffer.read_string.encoding.should == ::Encoding::UTF_8
        end

        it 'decodes an empty string' do
          buffer = described_class.new("\x00\x00")
          buffer.read_string.should be_empty
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          buffer.read_string
          buffer.should eql_bytes("\xff\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = described_class.new(buffer.read(5))
          expect { b.read_string }.to raise_error(DecodingError)
        end
      end

      describe '#read_long_string' do
        let :buffer do
          described_class.new("\x00\x01\x00\00" << ('x' * 0x10000))
        end

        it 'decodes a string' do
          str = buffer.read_long_string
          str.should start_with('xxx')
          str.length.should == 0x10000
        end

        it 'decodes a string as UTF-8' do
          buffer.read_long_string.encoding.should == ::Encoding::UTF_8
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          buffer.read_long_string
          buffer.should eql_bytes("\xff\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = described_class.new(buffer.read(246))
          expect { b.read_long_string }.to raise_error(DecodingError)
        end
      end

      describe '#read_uuid' do
        let :buffer do
          described_class.new("\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11")
        end

        it 'decodes a UUID as a Cql::Uuid' do
          buffer.read_uuid.should == Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        end
        
        it 'decodes a UUID as a Cql::TimeUuid' do
          uuid = buffer.read_uuid(TimeUuid)
          uuid.should == TimeUuid.new('a4a70900-24e1-11df-8924-001ff3591711')
          uuid.should be_a(TimeUuid)
        end

        it 'consumes the bytes' do
          buffer.read_uuid
          buffer.should be_empty
        end
        
        it 'raises an error when there a not enough bytes in the buffer' do
          b = described_class.new(buffer.discard(2).read(5))
          expect { b.read_uuid }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_list' do
        let :buffer do
          described_class.new("\x00\x02\x00\x05hello\x00\x05world")
        end

        it 'decodes a string list' do
          buffer.read_string_list.should == %w[hello world]
        end

        it 'decodes an empty string list' do
          buffer = described_class.new("\x00\x00")
          buffer.read_string_list.should == []
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          buffer.read_string_list
          buffer.should eql_bytes("\xff\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = described_class.new(buffer.read(13))
          expect { b.read_string_list }.to raise_error(DecodingError)
        end
      end

      describe '#read_bytes' do
        let :buffer do
          described_class.new("\x00\x01\x00\x00" << ("\x42" * 0x10000))
        end

        it 'decodes a byte array' do
          buffer.read_bytes.should eql_bytes("\x42" * 0x10000)
        end

        it 'decodes an empty byte array' do
          buffer = described_class.new("\x00\x00\x00\x00")
          buffer.read_bytes.should be_empty
        end

        it 'decodes null' do
          buffer = described_class.new("\x80\x00\x00\x00")
          buffer.read_bytes.should be_nil
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          buffer.read_bytes
          buffer.should eql_bytes("\xab\xcd")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = described_class.new(buffer.read(10))
          expect { b.read_bytes }.to raise_error(DecodingError)
        end
      end

      describe '#read_short_bytes' do
        let :buffer do
          described_class.new("\x01\x00" << ("\x42" * 0x100))
        end

        it 'decodes a byte array' do
          buffer.read_short_bytes.should eql_bytes("\x42" * 0x100)
        end

        it 'decodes an empty byte array' do
          buffer = described_class.new("\x00\x00\x00\x00")
          buffer.read_short_bytes.should be_empty
        end

        it 'decodes null' do
          buffer = described_class.new("\x80\x00")
          buffer.read_short_bytes.should be_nil
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          buffer.read_short_bytes
          buffer.should eql_bytes("\xab\xcd")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = described_class.new(buffer.read(10))
          expect { b.read_short_bytes }.to raise_error(DecodingError)
        end
      end

      describe '#read_option' do
        it 'decodes an option ID and value with instructions from a block' do
          buffer = described_class.new("\x00\x01\x00\x03foo")
          id, value = buffer.read_option do |id, buffer|
            buffer.read_string
          end
          id.should == 1
          value.should == 'foo'
        end

        it 'decodes an option ID and nil value when there is no block' do
          buffer = described_class.new("\xaa\xbb")
          id, value = buffer.read_option
          id.should == 0xaabb
          value.should be_nil
        end

        it 'consumes the bytes' do
          buffer = described_class.new("\x00\x01\x00\x03\xab")
          id, value = buffer.read_option do |id, buffer|
            buffer.read_short
          end
          buffer.should eql_bytes("\xab")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = described_class.new("\xaa")
          expect { b.read_option }.to raise_error(DecodingError)
        end
      end

      describe '#read_inet' do
        it 'decodes an IPv4 + port pair' do
          buffer = described_class.new("\x04\x00\x00\x00\x00\x00\x00#R")
          ip_addr, port = buffer.read_inet
          ip_addr.should == IPAddr.new('0.0.0.0')
          port.should == 9042
        end

        it 'decodes an IPv6 + port pair' do
          buffer = described_class.new("\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00#R")
          ip_addr, port = buffer.read_inet
          ip_addr.should == IPAddr.new('::1')
          port.should == 9042
        end

        it 'consumes the bytes' do
          buffer = described_class.new("\x04\x00\x00\x00\x00\x00\x00#R\xff\xaa")
          buffer.read_inet
          buffer.should eql_bytes("\xff\xaa")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          buffer1 = described_class.new("\x04\x00\x00\x00\x00\x00\x00")
          expect { buffer1.read_inet }.to raise_error(DecodingError)
          buffer2 = described_class.new("\x04\x00\x00\x00")
          expect { buffer2.read_inet }.to raise_error(DecodingError)
        end
      end

      describe '#read_consistency' do
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
        }.each do |consistency, bytes|
          it "decodes #{consistency.to_s.upcase}" do
            buffer = described_class.new(bytes)
            buffer.read_consistency.should == consistency
          end
        end

        it 'raises an exception for an unknown consistency' do
          expect { CqlByteBuffer.new("\xff\xff").read_consistency }.to raise_error(DecodingError)
          expect { CqlByteBuffer.new("\x00\x0f").read_consistency }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_map' do
        let :buffer do
          described_class.new("\x00\x02\x00\x05hello\x00\x05world\x00\x03foo\x00\x03bar")
        end

        it 'decodes a string multimap' do
          buffer.read_string_map.should == {'hello' => 'world', 'foo' => 'bar'}
        end

        it 'decodes an empty string map' do
          buffer = described_class.new("\x00\x00")
          buffer.read_string_map.should == {}
        end

        it 'consumes the bytes' do
          buffer << "\xff"
          buffer.read_string_map
          buffer.should eql_bytes("\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = described_class.new(buffer.read(20))
          expect { b.read_string_map }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_multimap' do
        let :buffer do
          described_class.new("\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x02\x00\x06snappy\x00\x04gzip")
        end

        it 'decodes a string multimap' do
          buffer.read_string_multimap.should == {'CQL_VERSION' => ['3.0.0'], 'COMPRESSION' => ['snappy', 'gzip']}
        end

        it 'decodes an empty string multimap' do
          buffer = described_class.new("\x00\x00")
          buffer.read_string_multimap.should == {}
        end

        it 'consumes the bytes' do
          buffer << "\xff"
          buffer.read_string_multimap
          buffer.should eql_bytes("\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = described_class.new(buffer.read(40))
          expect { b.read_string_multimap }.to raise_error(DecodingError)
        end
      end

      describe '#append_int' do
        it 'encodes an int' do
          buffer.append_int(2323234234)
          buffer.should eql_bytes("\x8a\x79\xbd\xba")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_int(10)
          buffer.should eql_bytes("\xab\x00\x00\x00\x0a")
        end

        it 'returns the buffer' do
          result = buffer.append_int(2323234234)
          result.should equal(buffer)
        end
      end

      describe '#append_short' do
        it 'encodes a short' do
          buffer.append_short(0xabcd)
          buffer.should eql_bytes("\xab\xcd")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_short(10)
          buffer.should eql_bytes("\xab\x00\x0a")
        end

        it 'returns the buffer' do
          result = buffer.append_short(42)
          result.should equal(buffer)
        end
      end

      describe '#append_string' do
        it 'encodes a string' do
          buffer.append_string('hello')
          buffer.should eql_bytes("\x00\x05hello")
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = 'I love π'
          buffer.append_string(str)
          buffer.should eql_bytes("\xff\x00\x09I love π")
        end

        it 'encodes an empty string' do
          buffer.append_string('')
          buffer.should eql_bytes("\x00\x00")
        end

        it 'encodes a non-string' do
          buffer.append_string(42)
          buffer.should eql_bytes("\x00\x0242")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_string('foo')
          buffer.should eql_bytes("\xab\x00\x03foo")
        end

        it 'returns the buffer' do
          result = buffer.append_string('hello')
          result.should equal(buffer)
        end
      end

      describe '#append_long_string' do
        it 'encodes a string' do
          buffer.append_long_string('hello world ' * 100_000)
          buffer.read(45).should eql_bytes("\x00\x12\x4f\x80hello world hello world hello world hello")
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = 'I love π'
          buffer.append_long_string(str)
          buffer.should eql_bytes("\xff\x00\x00\x00\x09I love π")
        end

        it 'encodes an empty string' do
          buffer.append_long_string('')
          buffer.should eql_bytes("\x00\x00\x00\x00")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_long_string('foo')
          buffer.should eql_bytes("\xab\x00\x00\x00\x03foo")
        end

        it 'returns the buffer' do
          result = buffer.append_long_string('hello')
          result.should equal(buffer)
        end
      end

      describe '#append_uuid' do
        let :uuid do
          Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        end

        it 'encodes an UUID' do
          buffer.append_uuid(uuid)
          buffer.should eql_bytes("\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11")
        end

        it 'encodes a UUID as 16 bytes' do
          buffer.append_uuid(Uuid.new('00000000-24e1-11df-8924-001ff3591711'))
          buffer.size.should eql(16)
        end

        it 'appends to the buffer' do
          buffer << 'FOO'
          buffer.append_uuid(uuid)
          buffer.read(3).should eql_bytes('FOO')
        end

        it 'returns the buffer' do
          result = buffer.append_uuid(uuid)
          result.should equal(buffer)
        end
      end

      describe '#append_string_list' do
        it 'encodes a string list' do
          buffer.append_string_list(%w[foo bar hello world])
          buffer.should eql_bytes("\x00\x04\x00\x03foo\x00\x03bar\x00\x05hello\x00\x05world")
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = %w[I love π]
          buffer.append_string_list(str)
          buffer.should eql_bytes("\xff\x00\x03\x00\x01I\x00\x04love\x00\x02π")
        end

        it 'encodes an empty string list' do
          buffer.append_string_list([])
          buffer.should eql_bytes("\x00\x00")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_string_list(%w[foo bar])
          buffer.should eql_bytes("\xab\x00\x02\x00\x03foo\x00\x03bar")
        end

        it 'returns the buffer' do
          result = buffer.append_string_list(%w[foo])
          result.should equal(buffer)
        end
      end

      describe '#append_bytes' do
        it 'encodes a byte array' do
          buffer.append_bytes("\xaa" * 2000)
          buffer.should eql_bytes("\x00\x00\x07\xd0" << ("\xaa" * 2000))
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = 'I love π'
          buffer.append_bytes(str)
          buffer.should eql_bytes("\xff\x00\x00\x00\x09I love π")
        end

        it 'encodes nil' do
          buffer.append_bytes(nil)
          buffer.should eql_bytes("\xff\xff\xff\xff")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_bytes("\xf0\x0b\xbar")
          buffer.should eql_bytes("\xab\x00\x00\x00\x04\xf0\x0b\xbar")
        end

        it 'returns the buffer' do
          result = buffer.append_bytes("\xab")
          result.should equal(buffer)
        end
      end

      describe '#append_short_bytes' do
        it 'encodes a byte array' do
          buffer.append_short_bytes("\xaa\xbb\xcc")
          buffer.should eql_bytes("\x00\x03\xaa\xbb\xcc")
        end

        it 'encodes a string with multibyte characters' do
          buffer << "\xff"
          str = 'I love π'
          buffer.append_short_bytes(str)
          buffer.should eql_bytes("\xff\x00\x09I love π")
        end

        it 'encodes nil' do
          buffer.append_short_bytes(nil)
          buffer.should eql_bytes("\xff\xff")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_short_bytes("\xf0\x0b\xbar")
          buffer.should eql_bytes("\xab\x00\x04\xf0\x0b\xbar")
        end

        it 'returns the buffer' do
          result = buffer.append_short_bytes("\xab")
          result.should equal(buffer)
        end
      end

      describe '#append_consistency' do
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
            buffer.append_consistency(consistency)
            buffer.should eql_bytes(expected_encoding)
          end
        end

        it 'raises an exception for an unknown consistency' do
          expect { buffer.append_consistency(:foo) }.to raise_error(EncodingError)
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_consistency(:one)
          buffer.should eql_bytes("\xab\x00\x01")
        end

        it 'returns the buffer' do
          result = buffer.append_consistency(:quorum)
          result.should equal(buffer)
        end
      end

      describe '#append_string_map' do
        it 'encodes a string map' do
          buffer.append_string_map('HELLO' => 'world', 'foo' => 'bar')
          buffer.should eql_bytes("\x00\x02\x00\x05HELLO\x00\x05world\x00\x03foo\x00\x03bar")
        end

        it 'encodes an empty map' do
          buffer.append_string_map({})
          buffer.should eql_bytes("\x00\x00")
        end

        it 'appends to the buffer' do
          buffer << "\xab"
          buffer.append_string_map('foo' => 'bar')
          buffer.should eql_bytes("\xab\x00\x01\x00\x03foo\x00\x03bar")
        end

        it 'returns the buffer' do
          result = buffer.append_string_map('HELLO' => 'world')
          result.should equal(buffer)
        end
      end

      describe '#append_long' do
        it 'encodes a long' do
          buffer.append_long(0x0123456789)
          buffer.should eql_bytes("\x00\x00\x00\x01\x23\x45\x67\x89")
        end

        it 'appends to the buffer' do
          buffer << "\x99"
          buffer.append_long(0x0123456789)
          buffer.should eql_bytes("\x99\x00\x00\x00\x01\x23\x45\x67\x89")
        end

        it 'returns the buffer' do
          result = buffer.append_long(1)
          result.should equal(buffer)
        end
      end

      describe '#append_varint' do
        it 'encodes a variable length integer' do
          buffer.append_varint(1231312312331283012830129382342342412123)
          buffer.should eql_bytes("\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[")
        end

        it 'encodes a negative variable length integer' do
          buffer.append_varint(-234234234234)
          buffer.should eql_bytes("\xC9v\x8D:\x86")
        end

        it 'encodes a negative variable length integer' do
          buffer.append_varint(-1)
          buffer.should eql_bytes("\xff")
        end

        it 'appends to the buffer' do
          buffer << "\x99"
          buffer.append_varint(-234234234234)
          buffer.should eql_bytes("\x99\xC9v\x8D:\x86")
        end

        it 'returns the buffer' do
          result = buffer.append_varint(-234234234234)
          result.should equal(buffer)
        end
      end

      describe '#append_decimal' do
        it 'encodes a BigDecimal as a decimal' do
          buffer.append_decimal(BigDecimal.new('1042342234234.123423435647768234'))
          buffer.should eql_bytes("\x00\x00\x00\x12\r'\xFDI\xAD\x80f\x11g\xDCfV\xAA")
        end

        it 'appends to the buffer' do
          buffer << "\x99"
          buffer.append_decimal(BigDecimal.new('1042342234234.123423435647768234'))
          buffer.read(1).should eql_bytes("\x99")
        end

        it 'returns the buffer' do
          result = buffer.append_decimal(BigDecimal.new('3.14'))
          result.should equal(buffer)
        end
      end

      describe '#append_double' do
        it 'encodes a double' do
          buffer.append_double(10000.123123123)
          buffer.should eql_bytes("@\xC3\x88\x0F\xC2\x7F\x9DU")
        end

        it 'appends to the buffer' do
          buffer << 'BEFORE'
          buffer.append_double(10000.123123123)
          buffer.read(6).should eql_bytes('BEFORE')
        end

        it 'returns the buffer' do
          result = buffer.append_double(10000.123123123)
          result.should equal(buffer)
        end
      end

      describe '#append_float' do
        it 'encodes a float' do
          buffer.append_float(12.13)
          buffer.should eql_bytes("AB\x14{")
        end

        it 'appends to the buffer' do
          buffer << 'BEFORE'
          buffer.append_float(12.13)
          buffer.read(6).should eql_bytes('BEFORE')
        end

        it 'returns the buffer' do
          result = buffer.append_float(12.13)
          result.should equal(buffer)
        end
      end
    end
  end
end