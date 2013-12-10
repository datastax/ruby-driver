# encoding: utf-8

module Cql
  module Compression
    begin
      require 'snappy'

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
    rescue LoadError => e
      raise LoadError, %[Snappy support requires the "snappy" gem: #{e.message}], e.backtrace
    end
  end
end