# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

module Cassandra
  module Compression
    module Compressors
      # A compressor that uses the LZ4 compression library.
      #
      # @note This compressor requires the
      #   [lz4-ruby](http://rubygems.org/gems/lz4-ruby) gem (v0.3.2 or later
      #   required).
      # @note No need to instantiate this class manually, use `compression:
      #   :lz4` option when calling {Cassandra.cluster} and one will be created
      #   automatically for you.
      class Lz4 < Compressor
        # @return [String] `'lz4'`
        attr_reader :algorithm

        # @param [Integer] min_size (64) Don't compress frames smaller than
        #   this size (see {#compress?}).
        def initialize(min_size = 64)
          unless defined?(::LZ4::Raw)
            begin
              require 'lz4-ruby'
            rescue LoadError => e
              raise LoadError, %(LZ4 support requires the "lz4-ruby" gem: #{e.message}),
                    e.backtrace
            end
          end

          @algorithm = 'lz4'.freeze
          @min_size = min_size
        end

        # @return [true, false] will return false for frames smaller than the
        #   `min_size` given to the constructor.
        # @see Cassandra::Compression::Compressor#compress?
        def compress?(str)
          str.bytesize > @min_size
        end

        # @see Cassandra::Compression::Compressor#compress
        def compress(str)
          [str.bytesize, ::LZ4::Raw.compress(str.to_s).first].pack(BUFFER_FORMAT)
        end

        # @see Cassandra::Compression::Compressor#decompress
        def decompress(str)
          decompressed_size, compressed_data = str.to_s.unpack(BUFFER_FORMAT)
          ::LZ4::Raw.decompress(compressed_data, decompressed_size).first
        end

        private

        # @private
        BUFFER_FORMAT = 'Na*'.freeze
      end
    end
  end
end
