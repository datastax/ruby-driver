# encoding: utf-8

require 'spec_helper'
require 'cql/compression/compression_common'


module Cql
  module Compression
    begin
      require 'cql/compression/snappy_compressor'
    
      describe SnappyCompressor do
        include_examples 'compressor', 'snappy'
      end
    rescue LoadError => e
      describe 'SnappyCompressor' do
        it 'supports Snappy' do
          pending e.message
        end
      end
    end
  end
end