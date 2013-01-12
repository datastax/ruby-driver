# encoding: utf-8

module Cql
  class Uuid
    def initialize(n)
      case n
      when String
        @n = from_s(n)
      else
        @n = n
      end
    end

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

    def value
      @n
    end

    def eql?(other)
      self.value == other.value
    end
    alias_method :==, :eql?

    private

    def from_s(str)
      str = str.gsub('-', '')
      n = 0
      (str.length/2).times do |i|
        n = (n << 8) | str[i * 2, 2].to_i(16)
      end
      n
    end
  end
end