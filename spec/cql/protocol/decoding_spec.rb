# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe Decoding do
      describe '#read_byte!' do
        let :buffer do
          ByteBuffer.new("\xab")
        end

        it 'decodes a raw byte' do
          Decoding.read_byte!(buffer).should == 0xab
        end

        it 'consumes the byte' do
          Decoding.read_byte!(buffer)
          buffer.should be_empty
        end

        it 'raises an error when there is no byte available' do
          expect { Decoding.read_byte!(ByteBuffer.new) }.to raise_error(DecodingError)
        end
      end

      describe '#read_varint!' do
        it 'decodes a variable length integer' do
          buffer = ByteBuffer.new("\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[")
          Decoding.read_varint!(buffer, 17).should == 1231312312331283012830129382342342412123
        end

        it 'decodes a negative variable length integer' do
          buffer = ByteBuffer.new("\xC9v\x8D:\x86")
          Decoding.read_varint!(buffer, 5).should == -234234234234
        end

        it 'decodes an unsigned variable length integer' do
          buffer = ByteBuffer.new("\xC9v\x8D:\x86")
          Decoding.read_varint!(buffer, 5, false).should == 865277393542
        end

        it 'consumes the bytes' do
          buffer = ByteBuffer.new("\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[\x01\x02\x03")
          Decoding.read_varint!(buffer, 17)
          buffer.should eql_bytes("\x01\x02\x03")
        end

        it 'raises an error when there is not enough bytes available' do
          buffer = ByteBuffer.new("\xC9v\x8D:")
          expect { Decoding.read_varint!(buffer, 7) }.to raise_error(DecodingError)
        end
      end

      describe '#read_decimal!' do
        let :buffer do
          ByteBuffer.new("\x00\x00\x00\x12\r'\xFDI\xAD\x80f\x11g\xDCfV\xAA")
        end

        it 'decodes a decimal to a BigDecimal' do
          Decoding.read_decimal!(buffer).should == BigDecimal.new('1042342234234.123423435647768234')
        end

        it 'decodes a negative decimal' do
          buffer = ByteBuffer.new("\x00\x00\x00\x12\xF2\xD8\x02\xB6R\x7F\x99\xEE\x98#\x99\xA9V")
          Decoding.read_decimal!(buffer).should == BigDecimal.new('-1042342234234.123423435647768234')
        end

        it 'decodes a positive decimal with only fractions' do
          buffer = ByteBuffer.new("\x00\x00\x00\x13*\xF8\xC4\xDF\xEB]o")
          Decoding.read_decimal!(buffer).should == BigDecimal.new('0.0012095473475870063')
        end

        it 'decodes a negative decimal with only fractions' do
          buffer = ByteBuffer.new("\x00\x00\x00\x13\xD5\a;\x20\x14\xA2\x91")
          Decoding.read_decimal!(buffer).should == BigDecimal.new('-0.0012095473475870063')
        end

        it 'consumes the bytes' do
          buffer << 'HELLO'
          Decoding.read_decimal!(buffer, buffer.length - 5)
          buffer.should eql_bytes('HELLO')
        end

        it 'defaults to using the buffer length' do
          Decoding.read_decimal!(buffer.dup).should == Decoding.read_decimal!(buffer, buffer.length)
        end

        it 'raises an error when there is not enough bytes available' do
          b = ByteBuffer.new(buffer.read(3))
          expect { Decoding.read_decimal!(b, 7) }.to raise_error(DecodingError)
        end
      end

      describe '#read_long!' do
        it 'decodes a positive long' do
          buffer = ByteBuffer.new("\x00\x00\xca\xfe\xba\xbe\x00\x00")
          Decoding.read_long!(buffer).should == 0x0000cafebabe0000
        end

        it 'decodes a negative long' do
          buffer = ByteBuffer.new("\xff\xee\xdd\xcc\xbb\xaa\x99\x88")
          Decoding.read_long!(buffer).should == 0xffeeddccbbaa9988 - 0x10000000000000000
        end

        it 'consumes the bytes' do
          buffer = ByteBuffer.new("\xca\xfe\xba\xbe\xca\xfe\xba\xbe\xca\xfe\xba\xbe")
          Decoding.read_long!(buffer)
          buffer.should eql_bytes("\xca\xfe\xba\xbe")
        end

        it 'raises an error when there is not enough bytes available' do
          buffer = ByteBuffer.new("\xca\xfe\xba\xbe\x00")
          expect { Decoding.read_long!(buffer) }.to raise_error(DecodingError)
        end
      end

      describe '#read_double!' do
        it 'decodes a double' do
          buffer = ByteBuffer.new("@\xC3\x88\x0F\xC2\x7F\x9DU")
          Decoding.read_double!(buffer).should == 10000.123123123
        end

        it 'consumes the bytes' do
          buffer = ByteBuffer.new("@\xC3\x88\x0F\xC2\x7F\x9DUxyz")
          Decoding.read_double!(buffer)
          buffer.should eql_bytes('xyz')
        end

        it 'raises an error when there is not enough bytes available' do
          buffer = ByteBuffer.new("@\xC3\x88\x0F")
          expect { Decoding.read_double!(buffer) }.to raise_error(DecodingError)
        end
      end

      describe '#read_float!' do
        it 'decodes a float' do
          buffer = ByteBuffer.new("AB\x14{")
          Decoding.read_float!(buffer).should be_within(0.00001).of(12.13)
        end

        it 'consumes the bytes' do
          buffer = ByteBuffer.new("AB\x14{xyz")
          Decoding.read_float!(buffer)
          buffer.should eql_bytes('xyz')
        end

        it 'raises an error when there is not enough bytes available' do
          buffer = ByteBuffer.new("\x0F")
          expect { Decoding.read_float!(buffer) }.to raise_error(DecodingError)
        end
      end

      describe '#read_int!' do
        let :buffer do
          ByteBuffer.new("\x00\xff\x00\xff")
        end

        it 'decodes a positive int' do
          Decoding.read_int!(buffer).should == 0x00ff00ff
        end

        it 'decodes a negative int' do
          buffer = ByteBuffer.new("\xff\xee\xdd\xcc")
          Decoding.read_int!(buffer).should == 0xffeeddcc - 0x100000000
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          Decoding.read_int!(buffer)
          buffer.should eql_bytes("\xab\xcd")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          buffer = ByteBuffer.new("\x01\xab")
          expect { Decoding.read_int!(buffer) }.to raise_error(DecodingError)
        end
      end

      describe '#read_short!' do
        let :buffer do
          ByteBuffer.new("\x00\x02")
        end

        it 'decodes a short' do
          Decoding.read_short!(buffer).should == 2
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          Decoding.read_short!(buffer)
          buffer.should eql_bytes("\xff\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          buffer = ByteBuffer.new("\x01")
          expect { Decoding.read_short!(buffer) }.to raise_error(DecodingError)
        end
      end

      describe '#read_string!' do
        let :buffer do
          ByteBuffer.new("\x00\x0bhej och hå")
        end

        it 'decodes a string' do
          Decoding.read_string!(buffer).should == 'hej och hå'.force_encoding(::Encoding::UTF_8)
        end

        it 'decodes a string as UTF-8' do
          Decoding.read_string!(buffer).encoding.should == ::Encoding::UTF_8
        end

        it 'decodes an empty string' do
          buffer = ByteBuffer.new("\x00\x00")
          Decoding.read_string!(buffer).should be_empty
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          Decoding.read_string!(buffer)
          buffer.should eql_bytes("\xff\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = ByteBuffer.new(buffer.read(5))
          expect { Decoding.read_string!(b) }.to raise_error(DecodingError)
        end
      end

      describe '#read_long_string!' do
        let :buffer do
          ByteBuffer.new("\x00\x01\x00\00" << ('x' * 0x10000))
        end

        it 'decodes a string' do
          Decoding.read_long_string!(buffer.dup).should start_with('xxx')
          Decoding.read_long_string!(buffer).length.should == 0x10000
        end

        it 'decodes a string as UTF-8' do
          Decoding.read_long_string!(buffer).encoding.should == ::Encoding::UTF_8
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          Decoding.read_long_string!(buffer)
          buffer.should eql_bytes("\xff\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = ByteBuffer.new(buffer.read(246))
          expect { Decoding.read_long_string!(b) }.to raise_error(DecodingError)
        end
      end

      describe '#read_uuid!' do
        let :buffer do
          ByteBuffer.new("\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11")
        end

        it 'decodes a UUID as a Cql::Uuid' do
          Decoding.read_uuid!(buffer).should == Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        end
        
        it 'decodes a UUID as a Cql::TimeUuid' do
          uuid = Decoding.read_uuid!(buffer, TimeUuid)
          uuid.should == TimeUuid.new('a4a70900-24e1-11df-8924-001ff3591711')
          uuid.should be_a(TimeUuid)
        end

        it 'consumes the bytes' do
          Decoding.read_uuid!(buffer)
          buffer.should be_empty
        end
        
        it 'raises an error when there a not enough bytes in the buffer' do
          b = ByteBuffer.new(buffer.discard(2).read(5))
          expect { Decoding.read_uuid!(b) }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_list!' do
        let :buffer do
          ByteBuffer.new("\x00\x02\x00\x05hello\x00\x05world")
        end

        it 'decodes a string list' do
          Decoding.read_string_list!(buffer).should == %w[hello world]
        end

        it 'decodes an empty string list' do
          buffer = ByteBuffer.new("\x00\x00")
          Decoding.read_string_list!(buffer).should == []
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          Decoding.read_string_list!(buffer)
          buffer.should eql_bytes("\xff\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = ByteBuffer.new(buffer.read(13))
          expect { Decoding.read_string_list!(b) }.to raise_error(DecodingError)
        end
      end

      describe '#read_bytes!' do
        let :buffer do
          ByteBuffer.new("\x00\x01\x00\x00" << ("\x42" * 0x10000))
        end

        it 'decodes a byte array' do
          Decoding.read_bytes!(buffer).should eql_bytes("\x42" * 0x10000)
        end

        it 'decodes an empty byte array' do
          buffer = ByteBuffer.new("\x00\x00\x00\x00")
          Decoding.read_bytes!(buffer).should be_empty
        end

        it 'decodes null' do
          buffer = ByteBuffer.new("\x80\x00\x00\x00")
          Decoding.read_bytes!(buffer).should be_nil
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          Decoding.read_bytes!(buffer)
          buffer.should eql_bytes("\xab\xcd")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = ByteBuffer.new(buffer.read(10))
          expect { Decoding.read_bytes!(b) }.to raise_error(DecodingError)
        end
      end

      describe '#read_short_bytes!' do
        let :buffer do
          ByteBuffer.new("\x01\x00" << ("\x42" * 0x100))
        end

        it 'decodes a byte array' do
          Decoding.read_short_bytes!(buffer).should eql_bytes("\x42" * 0x100)
        end

        it 'decodes an empty byte array' do
          buffer = ByteBuffer.new("\x00\x00\x00\x00")
          Decoding.read_short_bytes!(buffer).should be_empty
        end

        it 'decodes null' do
          buffer = ByteBuffer.new("\x80\x00")
          Decoding.read_short_bytes!(buffer).should be_nil
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          Decoding.read_short_bytes!(buffer)
          buffer.should eql_bytes("\xab\xcd")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = ByteBuffer.new(buffer.read(10))
          expect { Decoding.read_short_bytes!(b) }.to raise_error(DecodingError)
        end
      end

      describe '#read_option!' do
        it 'decodes an option ID and value with instructions from a block' do
          buffer = ByteBuffer.new("\x00\x01\x00\x03foo")
          id, value = Decoding.read_option!(buffer) do |id, buffer|
            Decoding.read_string!(buffer)
          end
          id.should == 1
          value.should == 'foo'
        end

        it 'decodes an option ID and nil value when there is no block' do
          buffer = ByteBuffer.new("\xaa\xbb")
          id, value = Decoding.read_option!(buffer)
          id.should == 0xaabb
          value.should be_nil
        end

        it 'consumes the bytes' do
          buffer = ByteBuffer.new("\x00\x01\x00\x03\xab")
          id, value = Decoding.read_option!(buffer) do |id, buffer|
            Decoding.read_short!(buffer)
          end
          buffer.should eql_bytes("\xab")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          buffer = ByteBuffer.new("\xaa")
          expect { Decoding.read_option!(buffer) }.to raise_error(DecodingError)
        end
      end

      describe '#read_inet!' do
        it 'decodes an IPv4 + port pair' do
          buffer = ByteBuffer.new("\x04\x00\x00\x00\x00\x00\x00#R")
          ip_addr, port = Decoding.read_inet!(buffer)
          ip_addr.should == IPAddr.new('0.0.0.0')
          port.should == 9042
        end

        it 'decodes an IPv6 + port pair' do
          buffer = ByteBuffer.new("\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00#R")
          ip_addr, port = Decoding.read_inet!(buffer)
          ip_addr.should == IPAddr.new('::1')
          port.should == 9042
        end

        it 'consumes the bytes' do
          buffer = ByteBuffer.new("\x04\x00\x00\x00\x00\x00\x00#R\xff\xaa")
          Decoding.read_inet!(buffer)
          buffer.should eql_bytes("\xff\xaa")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          buffer1 = ByteBuffer.new("\x04\x00\x00\x00\x00\x00\x00")
          expect { Decoding.read_inet!(buffer1) }.to raise_error(DecodingError)
          buffer2 = ByteBuffer.new("\x04\x00\x00\x00")
          expect { Decoding.read_inet!(buffer2) }.to raise_error(DecodingError)
        end
      end

      describe '#read_consistency!' do
        it 'decodes ANY' do
          buffer = ByteBuffer.new("\x00\x00")
          Decoding.read_consistency!(buffer).should == :any
        end

        it 'decodes ONE' do
          buffer = ByteBuffer.new("\x00\x01")
          Decoding.read_consistency!(buffer).should == :one
        end

        it 'decodes TWO' do
          buffer = ByteBuffer.new("\x00\x02")
          Decoding.read_consistency!(buffer).should == :two
        end

        it 'decodes THREE' do
          buffer = ByteBuffer.new("\x00\x03")
          Decoding.read_consistency!(buffer).should == :three
        end

        it 'decodes QUORUM' do
          buffer = ByteBuffer.new("\x00\x04")
          Decoding.read_consistency!(buffer).should == :quorum
        end

        it 'decodes ALL' do
          buffer = ByteBuffer.new("\x00\x05")
          Decoding.read_consistency!(buffer).should == :all
        end

        it 'decodes LOCAL_QUORUM' do
          buffer = ByteBuffer.new("\x00\x06")
          Decoding.read_consistency!(buffer).should == :local_quorum
        end

        it 'decodes EACH_QUORUM' do
          buffer = ByteBuffer.new("\x00\x07")
          Decoding.read_consistency!(buffer).should == :each_quorum
        end

        it 'decodes SERIAL' do
          buffer = ByteBuffer.new("\x00\x08")
          Decoding.read_consistency!(buffer).should == :serial
        end

        it 'decodes LOCAL_SERIAL' do
          buffer = ByteBuffer.new("\x00\x09")
          Decoding.read_consistency!(buffer).should == :local_serial
        end

        it 'decodes LOCAL_ONE' do
          buffer = ByteBuffer.new("\x00\x0a")
          Decoding.read_consistency!(buffer).should == :local_one
        end

        it 'raises an exception for an unknown consistency' do
          expect { Decoding.read_consistency!(ByteBuffer.new("\xff\xff")) }.to raise_error(DecodingError)
          expect { Decoding.read_consistency!(ByteBuffer.new("\x00\x0f")) }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_map!' do
        let :buffer do
          ByteBuffer.new("\x00\x02\x00\x05hello\x00\x05world\x00\x03foo\x00\x03bar")
        end

        it 'decodes a string multimap' do
          Decoding.read_string_map!(buffer).should == {'hello' => 'world', 'foo' => 'bar'}
        end

        it 'decodes an empty string map' do
          buffer = ByteBuffer.new("\x00\x00")
          Decoding.read_string_map!(buffer).should == {}
        end

        it 'consumes the bytes' do
          buffer << "\xff"
          Decoding.read_string_map!(buffer)
          buffer.should eql_bytes("\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = ByteBuffer.new(buffer.read(20))
          expect { Decoding.read_string_map!(b) }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_multimap!' do
        let :buffer do
          ByteBuffer.new("\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x02\x00\x06snappy\x00\x04gzip")
        end

        it 'decodes a string multimap' do
          Decoding.read_string_multimap!(buffer).should == {'CQL_VERSION' => ['3.0.0'], 'COMPRESSION' => ['snappy', 'gzip']}
        end

        it 'decodes an empty string multimap' do
          buffer = ByteBuffer.new("\x00\x00")
          Decoding.read_string_multimap!(buffer).should == {}
        end

        it 'consumes the bytes' do
          buffer << "\xff"
          Decoding.read_string_multimap!(buffer)
          buffer.should eql_bytes("\xff")
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          b = ByteBuffer.new(buffer.read(40))
          expect { Decoding.read_string_multimap!(b) }.to raise_error(DecodingError)
        end
      end
    end
  end
end