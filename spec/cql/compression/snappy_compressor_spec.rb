# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
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

require 'spec_helper'
require 'cql/compression/compression_common'


module Cql
  module Compression
    begin
      require 'cql/compression/snappy_compressor'
    
      describe SnappyCompressor do
        include_examples 'compressor', 'snappy', "\x19\x10helloN\x05\x00"
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