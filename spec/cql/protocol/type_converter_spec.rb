# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe TypeConverter do
      let :converter do
        described_class.new
      end

      let :buffer do
        CqlByteBuffer.new
      end

      describe '#to_bytes' do
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
    end
  end
end