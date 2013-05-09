# encoding: utf-8

module Cql
  class ByteBuffer
    def initialize(initial_bytes='')
      @bytes = initial_bytes.force_encoding(::Encoding::BINARY)
      @offset = 0
    end

    def length
      @bytes.bytesize - @offset
    end
    alias_method :size, :length
    alias_method :bytesize, :length

    def empty?
      length == 0
    end

    def append(bytes)
      if @offset >= 2**20
        @bytes.slice!(0, @offset)
        @offset = 0
      end
      bytes = bytes.to_s
      unless bytes.ascii_only?
        bytes = bytes.dup.force_encoding(::Encoding::BINARY)
      end
      retag = @bytes.empty?
      @bytes << bytes
      @bytes.force_encoding(::Encoding::BINARY) if retag
      self
    end
    alias_method :<<, :append

    def discard(n)
      @offset += n
      self
    end

    def read(n)
      raise RangeError, "#{n} bytes required but only #{length} available" if length < n
      s = @bytes[@offset, n]
      discard(n)
      s
    end

    def read_int
      raise RangeError, "4 bytes required to read an int, but only #{length} available" if length < 4
      i0 = @bytes.getbyte(@offset + 0)
      i1 = @bytes.getbyte(@offset + 1)
      i2 = @bytes.getbyte(@offset + 2)
      i3 = @bytes.getbyte(@offset + 3)
      discard(4)
      (i0 << 24) | (i1 << 16) | (i2 << 8) | i3
    end

    def read_short
      raise RangeError, "2 bytes required to read a short, but only #{length} available" if length < 2
      i0 = @bytes.getbyte(@offset + 0)
      i1 = @bytes.getbyte(@offset + 1)
      discard(2)
      (i0 << 8) | i1
    end

    def read_byte(signed=false)
      raise RangeError, "No bytes available to read byte" if empty?
      b = @bytes.getbyte(@offset)
      b = (b & 0x7f) - (b & 0x80) if signed
      discard(1)
      b
    end

    def eql?(other)
      other.to_str.eql?(self.to_str)
    end
    alias_method :==, :eql?

    def hash
      @bytes.hash
    end

    def dup
      self.class.new(@bytes.dup)
    end

    def to_str
      @bytes[@offset, length]
    end
    alias_method :to_s, :to_str

    def inspect
      %(#<#{self.class.name}: #{to_str.inspect}>)
    end

    private

    INT_FORMAT = 'N'.freeze
    SHORT_FORMAT = 'n'.freeze
  end
end