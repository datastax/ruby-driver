# encoding: utf-8

require 'spec_helper'
require 'cql/compression/compression_common'


module Cql
  module Compression
    begin
      require 'cql/compression/lz4_compressor'

      describe Lz4Compressor do
        include_examples 'compressor', 'lz4', "\x00\x00\x01\xF4[hello\x05\x00Phello"
      end
    rescue LoadError => e
      describe 'Lz4Compressor' do
        it 'supports LZ4' do
          pending e.message
        end
      end
    end
  end
end