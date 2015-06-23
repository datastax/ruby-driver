# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
      # A compressor that uses the Snappy compression library.
      #
      # @note This compressor requires the
      #   [snappy](http://rubygems.org/gems/snappy) gem (v0.0.10 or later for
      #   JRuby support).
      # @note No need to instantiate this class manually, use `compression:
      #   :snappy` option when calling {Cassandra.cluster} and one will be
      #   created automatically for you.
      class Snappy < Compressor
        # @return [String] `'snappy'`
        attr_reader :algorithm

        # @param [Integer] min_size (64) Don't compress frames smaller than
        #   this size (see {#compress?}).
        def initialize(min_size=64)
          unless defined?(::Snappy)
            begin
              require 'snappy'
            rescue LoadError => e
              raise LoadError, %[Snappy support requires the "snappy" gem: #{e.message}], e.backtrace
            end
          end

          @algorithm = 'snappy'.freeze
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
          ::Snappy.deflate(str)
        end

        # @see Cassandra::Compression::Compressor#decompress
        def decompress(str)
          ::Snappy.inflate(str)
        end
      end
    end
  end
end
