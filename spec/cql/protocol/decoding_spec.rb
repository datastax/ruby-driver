# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe Decoding do
      describe '#read_byte!' do
        let :buffer do
          "\xab"
        end

        it 'decodes a raw byte' do
          Decoding.read_byte!(buffer).should == 0xab
        end

        it 'consumes the byte' do
          Decoding.read_byte!(buffer)
          buffer.should be_empty
        end

        it 'raises an error when there is no byte available' do
          expect { Decoding.read_byte!('') }.to raise_error(DecodingError)
        end
      end

      describe '#read_varint!' do
        it 'decodes a variable length integer' do
          Decoding.read_varint!("\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[", 17).should == 1231312312331283012830129382342342412123
        end

        it 'decodes a negative variable length integer' do
          Decoding.read_varint!("\xC9v\x8D:\x86", 5).should == -234234234234
        end

        it 'decodes an unsigned variable length integer' do
          Decoding.read_varint!("\xC9v\x8D:\x86", 5, false).should == 865277393542
        end

        it 'consumes the bytes' do
          buffer = "\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[\x01\x02\x03"
          Decoding.read_varint!(buffer, 17)
          buffer.should == "\x01\x02\x03"
        end

        it 'raises an error when there is not enough bytes available' do
          expect { Decoding.read_varint!("\xC9v\x8D:", 7) }.to raise_error(DecodingError)
        end
      end

      describe '#read_int!' do
        let :buffer do
          "\x00\xff\x00\xff"
        end

        it 'decodes an int' do
          Decoding.read_int!(buffer).should == 0x00ff00ff
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          Decoding.read_int!(buffer)
          buffer.should == "\xab\xcd"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_int!("\x01\xab") }.to raise_error(DecodingError)
        end
      end

      describe '#read_short!' do
        let :buffer do
          "\x00\x02"
        end

        it 'decodes a short' do
          Decoding.read_short!(buffer).should == 2
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          Decoding.read_short!(buffer)
          buffer.should == "\xff\xff"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_short!("\x01") }.to raise_error(DecodingError)
        end
      end

      describe '#read_string!' do
        let :buffer do
          "\x00\x0bhej och hå"
        end

        it 'decodes a string' do
          Decoding.read_string!(buffer).should == 'hej och hå'.force_encoding(::Encoding::UTF_8)
        end

        it 'decodes a string as UTF-8' do
          Decoding.read_string!(buffer).encoding.should == ::Encoding::UTF_8
        end

        it 'decodes an empty string' do
          Decoding.read_string!("\x00\x00").should be_empty
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          Decoding.read_string!(buffer)
          buffer.should == "\xff\xff"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_string!(buffer.slice(0, 5)) }.to raise_error(DecodingError)
        end
      end

      describe '#read_long_string!' do
        let :buffer do
          "\x00\x01\x00\00" << ('x' * 0x10000)
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
          buffer.should == "\xff\xff"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_long_string!(buffer.slice(0, 246)) }.to raise_error(DecodingError)
        end
      end

      describe '#read_uuid!' do
        let :buffer do
          "\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11"
        end

        it 'decodes a UUID' do
          Decoding.read_uuid!(buffer).should == Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        end
        
        it 'consumes the bytes' do
          Decoding.read_uuid!(buffer)
          buffer.should be_empty
        end
        
        it 'raises an error when there a not enough bytes in the buffer' do
          expect { Decoding.read_uuid!(buffer[2, 5]) }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_list!' do
        let :buffer do
          "\x00\x02\x00\x05hello\x00\x05world"
        end

        it 'decodes a string list' do
          Decoding.read_string_list!(buffer).should == %w[hello world]
        end

        it 'decodes an empty string list' do
          Decoding.read_string_list!("\x00\x00").should == []
        end

        it 'consumes the bytes' do
          buffer << "\xff\xff"
          Decoding.read_string_list!(buffer)
          buffer.should == "\xff\xff"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_string_list!(buffer.slice(0, 13)) }.to raise_error(DecodingError)
        end
      end

      describe '#read_bytes!' do
        let :buffer do
          "\x00\x01\x00\x00" << ("\x42" * 0x10000)
        end

        it 'decodes a byte array' do
          Decoding.read_bytes!(buffer).should == ("\x42" * 0x10000)
        end

        it 'decodes an empty byte array' do
          Decoding.read_bytes!("\x00\x00\x00\x00").should == ''
        end

        it 'returns an ASCII-8BIT encoded string' do
          Decoding.read_bytes!("\x00\x00\x00\x01\xaa").encoding.should == ::Encoding::BINARY
        end

        it 'decodes null' do
          Decoding.read_bytes!("\x80\x00\x00\x00").should be_nil
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          Decoding.read_bytes!(buffer)
          buffer.should == "\xab\xcd"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_bytes!(buffer[0, 10]) }.to raise_error(DecodingError)
        end
      end

      describe '#read_short_bytes!' do
        let :buffer do
          "\x01\x00" << ("\x42" * 0x100)
        end

        it 'decodes a byte array' do
          Decoding.read_short_bytes!(buffer).should == ("\x42" * 0x100)
        end

        it 'decodes an empty byte array' do
          Decoding.read_short_bytes!("\x00\x00\x00\x00").should == ''
        end

        it 'returns an ASCII-8BIT encoded string' do
          Decoding.read_short_bytes!("\x00\x00\x00\x01\xaa").encoding.should == ::Encoding::BINARY
        end

        it 'decodes null' do
          Decoding.read_short_bytes!("\x80\x00").should be_nil
        end

        it 'consumes the bytes' do
          buffer << "\xab\xcd"
          Decoding.read_short_bytes!(buffer)
          buffer.should == "\xab\xcd"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_short_bytes!(buffer[0, 10]) }.to raise_error(DecodingError)
        end
      end

      describe '#read_option!' do
        it 'decodes an option ID and value with instructions from a block' do
          id, value = Decoding.read_option!("\x00\x01\x00\x03foo") do |id, buffer|
            Decoding.read_string!(buffer)
          end
          id.should == 1
          value.should == 'foo'
        end

        it 'decodes an option ID and nil value when there is no block' do
          id, value = Decoding.read_option!("\xaa\xbb")
          id.should == 0xaabb
          value.should be_nil
        end

        it 'consumes the bytes' do
          buffer = "\x00\x01\x00\x03\xab"
          id, value = Decoding.read_option!(buffer) do |id, buffer|
            Decoding.read_short!(buffer)
          end
          buffer.should == "\xab"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_option!("\xaa") }.to raise_error(DecodingError)
        end
      end

      describe '#read_option_list!' do
        it 'decodes an option list'
        it 'consumes the bytes'
        it 'raises an error when there are not enough bytes in the buffer'
      end

      describe '#read_inet!' do
        it 'decodes an IPv4 + port pair' do
          ip_addr, port = Decoding.read_inet!("\x04\x00\x00\x00\x00\x00\x00#R")
          ip_addr.should == IPAddr.new('0.0.0.0')
          port.should == 9042
        end

        it 'decodes an IPv6 + port pair' do
          ip_addr, port = Decoding.read_inet!("\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00#R")
          ip_addr.should == IPAddr.new('::1')
          port.should == 9042
        end

        it 'consumes the bytes' do
          buffer = "\x04\x00\x00\x00\x00\x00\x00#R\xff\xaa"
          Decoding.read_inet!(buffer)
          buffer.should == "\xff\xaa"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_inet!("\x04\x00\x00\x00\x00\x00\x00") }.to raise_error(DecodingError)
          expect { Decoding.read_inet!("\x04\x00\x00\x00") }.to raise_error(DecodingError)
        end
      end

      describe '#read_consistency!' do
        it 'decodes ANY' do
          Decoding.read_consistency!("\x00\x00").should == :any
        end

        it 'decodes ONE' do
          Decoding.read_consistency!("\x00\x01").should == :one
        end

        it 'decodes TWO' do
          Decoding.read_consistency!("\x00\x02").should == :two
        end

        it 'decodes THREE' do
          Decoding.read_consistency!("\x00\x03").should == :three
        end

        it 'decodes QUORUM' do
          Decoding.read_consistency!("\x00\x04").should == :quorum
        end

        it 'decodes ALL' do
          Decoding.read_consistency!("\x00\x05").should == :all
        end

        it 'decodes LOCAL_QUORUM' do
          Decoding.read_consistency!("\x00\x06").should == :local_quorum
        end

        it 'decodes EACH_QUORUM' do
          Decoding.read_consistency!("\x00\x07").should == :each_quorum
        end

        it 'raises an exception for an unknown consistency' do
          expect { Decoding.read_consistency!("\xff\xff") }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_map!' do
        let :buffer do
          "\x00\x02\x00\x05hello\x00\x05world\x00\x03foo\x00\x03bar"
        end

        it 'decodes a string multimap' do
          Decoding.read_string_map!(buffer).should == {'hello' => 'world', 'foo' => 'bar'}
        end

        it 'decodes an empty string map' do
          Decoding.read_string_map!("\x00\x00").should == {}
        end

        it 'consumes the bytes' do
          buffer << "\xff"
          Decoding.read_string_map!(buffer)
          buffer.should == "\xff"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_string_map!(buffer.slice(0, 20)) }.to raise_error(DecodingError)
        end
      end

      describe '#read_string_multimap!' do
        let :buffer do
          "\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x02\x00\x06snappy\x00\x04gzip"
        end

        it 'decodes a string multimap' do
          Decoding.read_string_multimap!(buffer).should == {'CQL_VERSION' => ['3.0.0'], 'COMPRESSION' => ['snappy', 'gzip']}
        end

        it 'decodes an empty string multimap' do
          Decoding.read_string_multimap!("\x00\x00").should == {}
        end

        it 'consumes the bytes' do
          buffer << "\xff"
          Decoding.read_string_multimap!(buffer)
          buffer.should == "\xff"
        end

        it 'raises an error when there are not enough bytes in the buffer' do
          expect { Decoding.read_string_multimap!(buffer.slice(0, 40)) }.to raise_error(DecodingError)
        end
      end
    end
  end
end