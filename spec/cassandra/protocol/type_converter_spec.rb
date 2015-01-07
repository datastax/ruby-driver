# encoding: ascii-8bit

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
  module Protocol
    describe TypeConverter do
      let :converter do
        described_class.new
      end

      let :buffer do
        CqlByteBuffer.new
      end

      numeric_types = [
        :bigint,
        :counter,
        :decimal,
        :double,
        :float,
        :int,
        :varint,
      ]

      types = numeric_types + [
        :ascii,
        :blob,
        :boolean,
        :inet,
        :text,
        :varchar,
        :timestamp,
        :timeuuid,
        :uuid,
      ]

      describe '#to_bytes' do
        context 'when encoding normal value' do
          types.each do |type|
            it "encodes a null #{type.upcase}" do
              converter.to_bytes(buffer, type, nil, 4).should eql_bytes("\xff\xff\xff\xff")
            end
          end

          it 'encodes a null LIST' do
            converter.to_bytes(buffer, [:list, :int], nil, 4).should eql_bytes("\xff\xff\xff\xff")
          end

          it 'encodes a null MAP' do
            converter.to_bytes(buffer, [:map, :text, :text], nil, 4).should eql_bytes("\xff\xff\xff\xff")

          end

          it 'encodes a null SET' do
            converter.to_bytes(buffer, [:set, :uuid], nil, 4).should eql_bytes("\xff\xff\xff\xff")
          end
        end

        context 'when encoding collection values' do
          types.each do |type|
            it "encodes a null #{type.upcase}" do
              converter.to_bytes(buffer, type, nil, 2).should eql_bytes("\xff\xff")
            end
          end
        end

        context 'when encoding and decoding negative numbers' do
          numeric_types.each do |type|
            it "encodes and decodes a -1 #{type.upcase}" do
              value = type == :decimal ? BigDecimal.new('-1') : -1
              encoded = converter.to_bytes(buffer, type, value, 4)
              converter.from_bytes(encoded, type, 4).should == value
            end
          end
        end
      end

      describe('#from_bytes') do
        context('with empty buffer') do
          it 'returns nil' do
            types.each do |type|
              expect(converter.from_bytes(buffer, type, size_bytes=4)).to be_nil
            end
          end
        end

        context("when decoding timestamp") do
          it "maintains microsenconds precision" do
            time = Time.now
            encoded = converter.to_bytes(buffer, :timestamp, time)
            decoded = converter.from_bytes(encoded, :timestamp)
            expect((decoded.to_r.to_f * 1000).to_i).to eq((time.to_r.to_f * 1000).to_i)
          end
        end
      end
    end
  end
end
