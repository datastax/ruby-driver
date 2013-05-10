# encoding: utf-8

module Cql
  class ByteBuffer
    def initialize(initial_bytes='')
      @read_buffer = ''
      @write_buffer = ''
      @offset = 0
      @length = 0
      append(initial_bytes) unless initial_bytes.empty?
    end

    attr_reader :length
    alias_method :size, :length
    alias_method :bytesize, :length

    def empty?
      length == 0
    end

    def append(bytes)
      bytes = bytes.to_s
      unless bytes.ascii_only?
        bytes = bytes.dup.force_encoding(::Encoding::BINARY)
      end
      retag = @write_buffer.empty?
      @write_buffer << bytes
      @write_buffer.force_encoding(::Encoding::BINARY) if retag
      @length += bytes.bytesize
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
      if @offset >= @read_buffer.bytesize
        swap_buffers
        s = @read_buffer[@offset, n]
        @offset += n
        @length -= n
        s
      elsif @offset + n > @read_buffer.bytesize
        s = @read_buffer[@offset, @read_buffer.bytesize - @offset]
        n -= s.bytesize
        swap_buffers
        s << @read_buffer[@offset, n]
        @offset += n
        @length -= n
        s
      else
        s = @read_buffer[@offset, n]
        @offset += n
        @length -= n
        s
      end
    end

    def read_int
      raise RangeError, "4 bytes required to read an int, but only #{@length} available" if @length < 4
      if @read_buffer.bytesize >= @offset + 4
        i0 = @read_buffer.getbyte(@offset + 0)
        i1 = @read_buffer.getbyte(@offset + 1)
        i2 = @read_buffer.getbyte(@offset + 2)
        i3 = @read_buffer.getbyte(@offset + 3)
        @offset += 4
        @length -= 4
      else
        i0 = read_byte
        i1 = read_byte
        i2 = read_byte
        i3 = read_byte
      end
      (i0 << 24) | (i1 << 16) | (i2 << 8) | i3
    end

    def read_short
      raise RangeError, "2 bytes required to read a short, but only #{@length} available" if @length < 2
      if @read_buffer.bytesize >= @offset + 2
        i0 = @read_buffer.getbyte(@offset + 0)
        i1 = @read_buffer.getbyte(@offset + 1)
        @offset += 2
        @length -= 2
      else
        i0 = read_byte
        i1 = read_byte
      end
      (i0 << 8) | i1
    end

    def read_byte(signed=false)
      raise RangeError, "No bytes available to read byte" if empty?
      if @offset >= @read_buffer.bytesize
        swap_buffers
      end
      b = @read_buffer.getbyte(@offset)
      b = (b & 0x7f) - (b & 0x80) if signed
      @offset += 1
      @length -= 1
      b
    end

    def eql?(other)
      self.bytes.eql?(other.bytes)
    end
    alias_method :==, :eql?

    def hash
      bytes.hash
    end

    def dup
      self.class.new(to_str)
    end

    def to_str
      bytes
    end
    alias_method :to_s, :to_str

    def inspect
      %(#<#{self.class.name}: #{to_str.inspect}>)
    end

    protected

    def bytes
      (@read_buffer + @write_buffer)[@offset, @length]
    end

    private

    MAX_OFFSET = 2**20
    INT_FORMAT = 'N'.freeze
    SHORT_FORMAT = 'n'.freeze

    def swap_buffers
      @offset -= @read_buffer.bytesize
      @read_buffer = @write_buffer
      @write_buffer = ''
    end
  end
end