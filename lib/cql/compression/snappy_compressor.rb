# encoding: utf-8

begin
  require 'snappy'
rescue LoadError => e
  raise LoadError, %[Snappy support requires the "snappy" gem: #{e.message}], e.backtrace
end

module Cql
  module Compression

    class SnappyCompressor
      attr_reader :algorithm

      def initialize(min_size=64)
        @algorithm = 'snappy'.freeze
        @min_size = min_size
      end

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