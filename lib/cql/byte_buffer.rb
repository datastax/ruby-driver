# encoding: utf-8

module Cql
  class ByteBuffer
    attr_reader :length
    alias_method :size, :length
    alias_method :bytesize, :length

    def initialize(initial_bytes='')
      @bytes = ''
      @offset = 0
      @length = 0
      append(initial_bytes) unless initial_bytes.empty?
    end

    def empty?
      @length == 0
    end

    def append(bytes)
      if @offset >= MAX_OFFSET
        @bytes = '' << to_s
        @offset = 0
        @length = @bytes.bytesize
      end
      bytes = bytes.to_s
      unless bytes.ascii_only?
        bytes = bytes.dup.force_encoding(::Encoding::BINARY)
      end
      retag = @bytes.empty?
      @bytes << bytes
      @length += bytes.bytesize
      @bytes.force_encoding(::Encoding::BINARY) if retag
      self
    end
    alias_method :<<, :append

    def discard(n)
      raise RangeError, "#{n} bytes to discard but only #{@length} available" if @length < n
      @offset += n
      @length -= n
      self
    end

    def read(n)
      raise RangeError, "#{n} bytes required but only #{@length} available" if @length < n
      s = @bytes[@offset, n]
      discard(n)
      s
    end

    def read_int
      raise RangeError, "4 bytes required to read an int, but only #{@length} available" if @length < 4
      i0 = @bytes.getbyte(@offset + 0)
      i1 = @bytes.getbyte(@offset + 1)
      i2 = @bytes.getbyte(@offset + 2)
      i3 = @bytes.getbyte(@offset + 3)
      discard(4)
      (i0 << 24) | (i1 << 16) | (i2 << 8) | i3
    end

    def read_short
      raise RangeError, "2 bytes required to read a short, but only #{@length} available" if @length < 2
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
      self.bytes.eql?(other.bytes)
    end
    alias_method :==, :eql?

    def hash
      @bytes.hash
    end

    def dup
      self.class.new(to_str)
    end

    def to_str
      bytes.dup
    end
    alias_method :to_s, :to_str

    def inspect
      %(#<#{self.class.name}: #{to_str.inspect}>)
    end

    protected

    def bytes
      @bytes[@offset, @length]
    end

    private

    MAX_OFFSET = 2**20
    INT_FORMAT = 'N'.freeze
    SHORT_FORMAT = 'n'.freeze
  end
end