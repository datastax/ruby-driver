# encoding: utf-8

begin
  require 'snappy'
rescue LoadError => e
  raise LoadError, %[Snappy support requires the "snappy" gem: #{e.message}], e.backtrace
end

module Cql
  module Compression

    # A compressor that uses the Snappy compression library.
    #
    # @note This compressor requires the [snappy](http://rubygems.org/gems/snappy)
    #   gem (v0.0.10 or later for JRuby support).
    class SnappyCompressor
      # @return [String]
      attr_reader :algorithm

      # @param [Integer] min_size (64) Don't compress frames smaller than
      #   this size (see {#compress?}).
      def initialize(min_size=64)
        @algorithm = 'snappy'.freeze
        @min_size = min_size
      end

      # @return [true, false] will return false for frames smaller than the
      #   `min_size` given to the constructor.
      def compress?(str)
        str.bytesize > @min_size
      end

      def compress(str)
        Snappy.deflate(str)
      end

      def decompress(str)
        Snappy.inflate(str)
      end
    end
  end
end