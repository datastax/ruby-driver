# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe TypeConverter do
      let :converter do
        described_class.new
      end

      let :buffer do
        ''
      end

      TYPES = [:ascii, :bigint, :blob, :boolean, :counter, :decimal, :double, :float, :inet, :int, :text, :varchar, :timestamp, :timeuuid, :uuid, :varint].freeze

      describe '#to_bytes' do
        context 'when encoding normal value' do
          TYPES.each do |type|
            it "encodes a null #{type.upcase}" do
              converter.to_bytes(buffer, type, nil, 4).should == "\xff\xff\xff\xff"
            end
          end

          it 'encodes a null LIST' do
            converter.to_bytes(buffer, [:list, :int], nil, 4).should == "\xff\xff\xff\xff"
          end

          it 'encodes a null MAP' do
            converter.to_bytes(buffer, [:map, :text, :text], nil, 4).should == "\xff\xff\xff\xff"

          end

          it 'encodes a null SET' do
            converter.to_bytes(buffer, [:set, :uuid], nil, 4).should == "\xff\xff\xff\xff"

          end
        end

        context 'when encoding collection values' do
          TYPES.each do |type|
            it "encodes a null #{type.upcase}" do
              converter.to_bytes(buffer, type, nil, 2).should == "\xff\xff"
            end
          end
        end
      end
    end
  end
end