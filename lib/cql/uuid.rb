# encoding: utf-8

module Cql
  # Represents a UUID value.
  #
  # This is a very basic implementation of UUIDs and exists more or less just
  # to encode and decode UUIDs from and to Cassandra.
  #
  # If you want to generate UUIDs see {Cql::TimeUuid::Generator}.
  #
  class Uuid
    # Creates a new UUID either from a string (expected to be on the standard
    # 8-4-4-4-12 form, or just 32 characters without hyphens), or from a
    # 128 bit number.
    #
    # @raise [ArgumentError] if the string does not conform to the expected format
    #
    def initialize(n)
      case n
      when String
        @n = from_s(n)
      else
        @n = n
      end
    end

    # Returns a string representation of this UUID in the standard 8-4-4-4-12 form.
    #
    def to_s
      @s ||= begin
        parts = []
        parts << (@n >> (24 * 4)).to_s(16).rjust(8, '0')
        parts << ((@n >> (20 * 4)) & 0xffff).to_s(16).rjust(4, '0')
        parts << ((@n >> (16 * 4)) & 0xffff).to_s(16).rjust(4, '0')
        parts << ((@n >> (12 * 4)) & 0xffff).to_s(16).rjust(4, '0')
        parts << (@n & 0xffffffffffff).to_s(16).rjust(12, '0')
        parts.join('-').force_encoding(::Encoding::ASCII)
      end
    end

    def hash
      @h = (@n & 0xffffffffffffffff) ^ ((@n >> 64) & 0xffffffffffffffff)
    end

    # Returns the numerical representation of this UUID
    #
    # @return [Bignum] the 128 bit numerical representation
    #
    def value
      @n
    end

    # @private
    def eql?(other)
      other.kind_of?(Uuid) && self.value == other.value
    end
    alias_method :==, :eql?

    private

    HEX_STRING_PATTERN = /^[0-9a-fA-F]+$/

    def from_s(str)
      str = str.gsub('-', '')
      raise ArgumentError, "Expected 32 chars but got #{str.length}" unless str.length == 32
      raise ArgumentError, "Expected only characters 0-9, a-f, A-F" unless str =~ HEX_STRING_PATTERN
      n = 0
      (str.length/2).times do |i|
        n = (n << 8) | str[i * 2, 2].to_i(16)
      end
      n
    end
  end
end
