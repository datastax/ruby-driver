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
  describe Util do
    describe '.guess_type' do
      specs = [
        [Types.text, 'test'],
        [Types.varint, 2**64],
        [Types.bigint, 2**63],
        [Types.boolean, true],
        [Types.boolean, false],
        [Types.decimal, BigDecimal('1042342234234.123423435647768234')],
        [Types.double, 10000.123123123],
        [Types.inet, ::IPAddr.new('8.8.8.8')],
        [Types.uuid, Uuid.new(::SecureRandom.hex)],
        [Types.uuid, TimeUuid.new(::SecureRandom.hex)],
        [Types.timestamp, ::Time.now],
        [Types.map(Types.text, Types.bigint), { 'text' => 0 }],
        [Types.list(Types.varint), [2**64]],
        [Types.set(Types.bigint), Set.new([0])],
      ]

      specs.each do |expected_type, value|
        it "returns #{expected_type} for #{value}" do
          expect(Util.guess_type(value)).to eq(expected_type)
        end
      end
    end

    describe '.decode_zigzag' do
      it 'should handle zero' do
        Util.decode_zigzag(0).should == 0
      end

      it 'should return expected positive values for even numbers' do
        (2..100).step(2) do |x|
          Util.decode_zigzag(x).should == x/2
        end
      end

      it 'should return expected negative values for odd numbers' do
        (1..99).step(2) do |x|
          Util.decode_zigzag(x).should == -(x + 1)/2
        end
      end
    end

    describe '.encode_zigzag32' do
      it 'should create (32-bit) values which can be successfully decoded' do
        (-200..200).each do |x|
          encoded = Util.encode_zigzag32(x)
          Util.decode_zigzag(encoded).should == x
        end
      end
    end

    describe '.encode_zigzag64' do
      it 'should create (64-bit) values which can be successfully decoded' do
        (-200..200).each do |x|
          encoded = Util.encode_zigzag64(x)
          Util.decode_zigzag(encoded).should == x
        end
      end
    end
  end
end
