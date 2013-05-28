# encoding: utf-8

require 'spec_helper'


module Cql
  describe ByteBuffer do
    let :buffer do
      described_class.new
    end

    describe '#initialize' do
      it 'can be inititialized empty' do
        described_class.new.should be_empty
      end

      it 'can be initialized with bytes' do
        described_class.new('hello').length.should == 5
      end
    end

    describe '#length/#size/#bytesize' do
      it 'returns the number of bytes in the buffer' do
        buffer << 'foo'
        buffer.length.should == 3
      end

      it 'is zero initially' do
        buffer.length.should == 0
      end

      it 'is aliased as #size' do
        buffer << 'foo'
        buffer.size.should == 3
      end

      it 'is aliased as #bytesize' do
        buffer << 'foo'
        buffer.bytesize.should == 3
      end
    end

    describe '#empty?' do
      it 'is true initially' do
        buffer.should be_empty
      end

      it 'is false when there are bytes in the buffer' do
        buffer << 'foo'
        buffer.should_not be_empty
      end
    end

    describe '#append/#<<' do
      it 'adds bytes to the buffer' do
        buffer.append('foo')
        buffer.should_not be_empty
      end

      it 'can be used as <<' do
        buffer << 'foo'
        buffer.should_not be_empty
      end

      it 'returns itself' do
        buffer.append('foo').should eql(buffer)
      end

      it 'stores its bytes as binary' do
        buffer.append('hällö').length.should == 7
        buffer.to_s.encoding.should == ::Encoding::BINARY
      end

      it 'handles appending with multibyte strings' do
        buffer.append('hello')
        buffer.append('würld')
        buffer.to_s.should == 'hellowürld'.force_encoding(::Encoding::BINARY)
      end

      it 'handles appending with another byte buffer' do
        buffer.append('hello ').append(ByteBuffer.new('world'))
        buffer.to_s.should == 'hello world'
      end
    end

    describe '#eql?' do
      it 'is equal to another buffer with the same contents' do
        b1 = described_class.new
        b2 = described_class.new
        b1.append('foo')
        b2.append('foo')
        b1.should eql(b2)
      end

      it 'is not equal to another buffer with other contents' do
        b1 = described_class.new
        b2 = described_class.new
        b1.append('foo')
        b2.append('bar')
        b1.should_not eql(b2)
      end

      it 'is aliased as #==' do
        b1 = described_class.new
        b2 = described_class.new
        b1.append('foo')
        b2.append('foo')
        b1.should == b2
      end

      it 'is equal to another buffer when both are empty' do
        b1 = described_class.new
        b2 = described_class.new
        b1.should eql(b2)
      end
    end

    describe '#hash' do
      it 'has the same hash code as another buffer with the same contents' do
        b1 = described_class.new
        b2 = described_class.new
        b1.append('foo')
        b2.append('foo')
        b1.hash.should == b2.hash
      end

      it 'is not equal to the hash code of another buffer with other contents' do
        b1 = described_class.new
        b2 = described_class.new
        b1.append('foo')
        b2.append('bar')
        b1.hash.should_not == b2.hash
      end

      it 'is equal to the hash code of another buffer when both are empty' do
        b1 = described_class.new
        b2 = described_class.new
        b1.hash.should == b2.hash
      end
    end

    describe '#to_s' do
      it 'returns the bytes' do
        buffer.append('hello world').to_s.should == 'hello world'
      end
    end

    describe '#to_str' do
      it 'returns the bytes' do
        buffer.append('hello world').to_str.should == 'hello world'
      end
    end

    describe '#inspect' do
      it 'returns the bytes wrapped in ByteBuffer(...)' do
        buffer.append("\xca\xfe")
        buffer.inspect.should == '#<Cql::ByteBuffer: "\xCA\xFE">'
      end
    end

    describe '#discard' do
      it 'discards the specified number of bytes from the front of the buffer' do
        buffer.append('hello world')
        buffer.discard(4)
        buffer.should == ByteBuffer.new('o world')
      end

      it 'returns the byte buffer' do
        buffer.append('hello world')
        buffer.discard(4).should == ByteBuffer.new('o world')
      end

      it 'raises an error if the number of bytes in the buffer is fewer than the number to discard' do
        expect { buffer.discard(1) }.to raise_error(RangeError)
        buffer.append('hello')
        expect { buffer.discard(7) }.to raise_error(RangeError)
      end
    end

    describe '#read' do
      it 'returns the specified number of bytes, as a string' do
        buffer.append('hello')
        buffer.read(4).should == 'hell'
      end

      it 'removes the bytes from the buffer' do
        buffer.append('hello')
        buffer.read(3)
        buffer.should == ByteBuffer.new('lo')
        buffer.read(2).should == 'lo'
      end

      it 'raises an error if there are not enough bytes' do
        buffer.append('hello')
        expect { buffer.read(23423543) }.to raise_error(RangeError)
        expect { buffer.discard(5).read(1) }.to raise_error(RangeError)
      end

      it 'returns a string with binary encoding' do
        buffer.append('hello')
        buffer.read(4).encoding.should == ::Encoding::BINARY
        buffer.append('∆')
        buffer.read(2).encoding.should == ::Encoding::BINARY
      end
    end

    describe '#read_int' do
      it 'returns the first four bytes interpreted as an int' do
        buffer.append("\xca\xfe\xba\xbe\x01")
        buffer.read_int.should == 0xcafebabe
      end

      it 'removes the bytes from the buffer' do
        buffer.append("\xca\xfe\xba\xbe\x01")
        buffer.read_int
        buffer.should == ByteBuffer.new("\x01")
      end

      it 'raises an error if there are not enough bytes' do
        buffer.append("\xca\xfe\xba")
        expect { buffer.read_int }.to raise_error(RangeError)
      end
    end

    describe '#read_short' do
      it 'returns the first two bytes interpreted as a short' do
        buffer.append("\xca\xfe\x01")
        buffer.read_short.should == 0xcafe
      end

      it 'removes the bytes from the buffer' do
        buffer.append("\xca\xfe\x01")
        buffer.read_short
        buffer.should == ByteBuffer.new("\x01")
      end

      it 'raises an error if there are not enough bytes' do
        buffer.append("\xca")
        expect { buffer.read_short }.to raise_error(RangeError)
      end
    end

    describe '#read_byte' do
      it 'returns the first bytes interpreted as an int' do
        buffer.append("\x10\x01")
        buffer.read_byte.should == 0x10
        buffer.read_byte.should == 0x01
      end

      it 'removes the byte from the buffer' do
        buffer.append("\x10\x01")
        buffer.read_byte
        buffer.should == ByteBuffer.new("\x01")
      end

      it 'raises an error if there are no bytes' do
        expect { buffer.read_byte }.to raise_error(RangeError)
      end

      it 'can interpret the byte as signed' do
        buffer.append("\x81\x02")
        buffer.read_byte(true).should == -127
        buffer.read_byte(true).should == 2
      end
    end

    describe '#dup' do
      it 'returns a copy' do
        buffer.append('hello world')
        copy = buffer.dup
        copy.should eql(buffer)
      end

      it 'returns a copy which can be modified without modifying the original' do
        buffer.append('hello world')
        copy = buffer.dup
        copy.append('goodbye')
        copy.should_not eql(buffer)
      end
    end

    describe '#cheap_peek' do
      it 'returns a prefix of the buffer' do
        buffer.append('foo')
        buffer.append('bar')
        buffer.read_byte
        buffer.append('hello')
        x = buffer.cheap_peek
        x.bytesize.should be > 0
        x.bytesize.should be <= buffer.bytesize
        buffer.to_str.should start_with(x)
      end
    end

    context 'when reading and appending' do
      it 'handles heavy churn' do
        1000.times do
          buffer.append('x' * 6)
          buffer.read_byte
          buffer.append('y')
          buffer.read_int
          buffer.read_short
          buffer.append('z' * 4)
          buffer.read_byte
          buffer.append('z')
          buffer.read_int
          buffer.should be_empty
        end
      end
    end
  end
end