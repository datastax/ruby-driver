# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
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

require 'spec_helper'
require 'cassandra/compression/common'


module Cassandra
  module Compression
    module Compressors
      begin
        require 'cassandra/compression/compressors/lz4'

        describe Lz4 do
          include_examples 'compressor', 'lz4', "\x00\x00\x01\xF4[hello\x05\x00Phello"
        end
      rescue LoadError => e
        describe 'Lz4' do
          it 'supports LZ4' do
            pending e.message
          end
        end
      end
    end
  end
end
