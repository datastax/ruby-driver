# encoding: utf-8

module Cql
  class ByteBuffer
    def initialize(initial_bytes='')
      @bytes = initial_bytes.force_encoding(::Encoding::BINARY)
    end

    def length
      @bytes.bytesize
    end
    alias_method :size, :length
    alias_method :bytesize, :length

    def empty?
      @bytes.empty?
    end

    def append(bytes)
      bytes = bytes.to_s
      unless bytes.ascii_only?
        bytes = bytes.dup.force_encoding(::Encoding::BINARY)
      end
      @bytes << bytes
      self
    end
    alias_method :<<, :append

    def discard(n)
      slice!(0, n)
    end

    def read_byte
      b = @bytes.getbyte(0)
      discard(1)
      b
    end

    def read(n)
      slice!(0, n).to_s
    end

    def slice!(start, length)
      raise RangeError, 'ByteBuffer#slice! can only work on the start of the buffer' unless start == 0
      slice = @bytes.slice!(start, length)
      self.class.new(slice)
    end

    def slice(offset, length=1)
      slice = @bytes[offset, length]
      self.class.new(slice)
    end
    alias_method :[], :slice

    def eql?(other)
      other.bytes.eql?(self.bytes)
    end
    alias_method :==, :eql?

    def hash
      @bytes.hash
    end

    def dup
      self.class.new(@bytes.dup)
    end

    def to_str
      @bytes
    end
    alias_method :to_s, :to_str

    def inspect
      %(#<#{self.class.name}: #{@bytes.inspect}>)
    end

    def unpack(pattern)
      @bytes.unpack(pattern)
    end

    def getbyte(n)
      @bytes.getbyte(n)
    end

    protected

    def bytes
      @bytes
    end
  end
end