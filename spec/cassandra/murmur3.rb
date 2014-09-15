# encoding: utf-8

#--
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
#++

require 'spec_helper'

module Cassandra
  describe(Murmur3) do
    describe('.hash') do
      [
        ['123', -7468325962851647638],
        ["\x00\xff\x10\xfa\x99" * 10, 5837342703291459765],
        ["\xfe" * 8, -8927430733708461935],
        ["\x10" * 8, 1446172840243228796],
        ['9223372036854775807', 7162290910810015547],
      ].each do |(string, hash)|
        context "with #{string.inspect}" do
          it "produces #{hash}" do
            expect(Cassandra::Murmur3.hash(string)).to eq(hash)
          end
        end
      end
    end
  end
end
