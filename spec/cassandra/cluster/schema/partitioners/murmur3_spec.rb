# encoding: utf-8

#--
# Copyright DataStax, Inc.
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
  class Cluster
    class Schema
      module Partitioners
        describe(Murmur3) do
          subject { Murmur3.new }

          describe('#create_token') do
            [
              ['123', -7468325962851647638],
              ["\x00\xff\x10\xfa\x99" * 10, 5837342703291459765],
              ["\xfe" * 8, -8927430733708461935],
              ["\x10" * 8, 1446172840243228796],
              ['9223372036854775807', 7162290910810015547],
            ].each do |(parition_key, token)|
              context "with #{parition_key.inspect}" do
                it "creates #{token}" do
                  expect(subject.create_token(parition_key)).to eq(token)
                end
              end
            end
          end

          describe('#parse_token') do
            [
              ['-7468325962851647638', -7468325962851647638],
              ['5837342703291459765', 5837342703291459765],
              ['-8927430733708461935', -8927430733708461935],
              ['1446172840243228796', 1446172840243228796],
              ['7162290910810015547', 7162290910810015547],
            ].each do |(string, token)|
              context "with #{string.inspect}" do
                it "creates #{token}" do
                  expect(subject.parse_token(string)).to eq(token)
                end
              end
            end
          end
        end
      end
    end
  end
end
