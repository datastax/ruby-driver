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
        s = RAW_FORMAT % @n
        s.insert(20, HYPHEN)
        s.insert(16, HYPHEN)
        s.insert(12, HYPHEN)
        s.insert( 8, HYPHEN)
        s
      end
    end

    def hash
      @n.hash
    end

    # Returns the numerical representation of this UUID
    #
    # @return [Bignum] the 128 bit numerical representation
    #
    def value
      @n
    end
    alias_method :to_i, :value

    # @private
    def eql?(other)
      other.respond_to?(:value) && self.value == other.value
    end
    alias_method :==, :eql?

    private

    RAW_FORMAT = '%032x'.freeze
    HYPHEN = '-'.freeze
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
