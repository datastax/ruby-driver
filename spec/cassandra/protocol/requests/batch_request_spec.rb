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
    describe BatchRequest do
      let :statement_id do
        "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f"
      end

      let :metadata do
        [['ks', 'tbl', 'things', :varchar], ['ks', 'tbl', 'things', :int]]
      end

      describe '#write' do
        let(:encoder) { V1::Encoder.new(nil, 2) }

        it 'encodes a BATCH request frame with a single prepared query' do
          batch = described_class.new(described_class::LOGGED_TYPE, :two)
          batch.add_prepared(statement_id, ['arg', 3], metadata.map(&:last))
          buffer = CqlByteBuffer.new
          bytes  = batch.write(buffer, 2, encoder)
          bytes.should eql_bytes(
            "\x00" +
            "\x00\x01" +
            "\x01" +
            "\x00\x10" + statement_id +
            "\x00\x02" + "\x00\x00\x00\x03arg" + "\x00\x00\x00\x04\x00\x00\x00\x03" +
            "\x00\x02"
          )
        end

        it 'encodes a BATCH request frame with a single query' do
          batch = described_class.new(described_class::LOGGED_TYPE, :one)
          batch.add_query(%<INSERT INTO things (a, b) VALUES ('foo', 3)>, [], [])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.should eql_bytes(
            "\x00" +
            "\x00\x01" +
            "\x00" +
            "\x00\x00\x00\x2bINSERT INTO things (a, b) VALUES ('foo', 3)" +
            "\x00\x00" +
            "\x00\x01"
          )
        end

        it 'encodes a BATCH request frame with a single query with on-the-fly bound values' do
          batch = described_class.new(described_class::LOGGED_TYPE, :two)
          batch.add_query('INSERT INTO things (a, b) VALUES (?, ?)', ['foo', 5], [:varchar, :bigint])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.should eql_bytes(
            "\x00" +
            "\x00\x01" +
            "\x00" +
            "\x00\x00\x00\x27INSERT INTO things (a, b) VALUES (?, ?)" +
            "\x00\x02" + "\x00\x00\x00\x03foo" + "\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x05" +
            "\x00\x02"
          )
        end

        it 'encodes a BATCH request frame with a a mix of prepared and non-prepared queries' do
          batch = described_class.new(described_class::LOGGED_TYPE, :two)
          batch.add_prepared(statement_id, ['arg', 3], [:varchar, :int])
          batch.add_query(%<INSERT INTO things (a, b) VALUES (?, ?)>, ['foo', 5], [:varchar, :bigint])
          batch.add_query(%<INSERT INTO things (a, b) VALUES ('foo', 3)>, [], [])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.should eql_bytes(
            "\x00" +
            "\x00\x03" +
            "\x01" +
            "\x00\x10" + statement_id +
            "\x00\x02" + "\x00\x00\x00\x03arg" + "\x00\x00\x00\x04\x00\x00\x00\x03" +
            "\x00" +
            "\x00\x00\x00\x27INSERT INTO things (a, b) VALUES (?, ?)" +
            "\x00\x02" + "\x00\x00\x00\x03foo" + "\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x05" +
            "\x00" +
            "\x00\x00\x00\x2bINSERT INTO things (a, b) VALUES ('foo', 3)" +
            "\x00\x00" +
            "\x00\x02"
          )
        end

        it 'encodes the type when it is "logged"' do
          batch = described_class.new(described_class::LOGGED_TYPE, :two)
          batch.add_prepared(statement_id, ['arg', 3], [:varchar, :int])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.to_s[0].should == "\x00"
        end

        it 'encodes the type when it is "unlogged"' do
          batch = described_class.new(described_class::UNLOGGED_TYPE, :two)
          batch.add_prepared(statement_id, ['arg', 3], [:varchar, :int])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.to_s[0].should == "\x01"
        end

        it 'encodes the type when it is "counter"' do
          batch = described_class.new(described_class::COUNTER_TYPE, :two)
          batch.add_prepared(statement_id, ['arg', 3], [:varchar, :int])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.to_s[0].should == "\x02"
        end

        it 'encodes the number of statements in the batch' do
          batch = described_class.new(described_class::LOGGED_TYPE, :two)
          batch.add_prepared(statement_id, ['arg', 3], [:varchar, :int])
          batch.add_prepared(statement_id, ['foo', 4], [:varchar, :int])
          batch.add_prepared(statement_id, ['bar', 5], [:varchar, :int])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.to_s[1, 2].should == "\x00\x03"
        end

        it 'encodes the type of each statement' do
          batch = described_class.new(described_class::LOGGED_TYPE, :two)
          batch.add_prepared(statement_id, ['arg', 3], [:varchar, :int])
          batch.add_query(%<INSERT INTO things (a, b) VALUES ('arg', 3)>, [], [])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.to_s[3, 1].should == "\x01"
          bytes.to_s[39, 1].should == "\x00"
        end

        it 'uses the type hints given to #add_query' do
          batch = described_class.new(described_class::LOGGED_TYPE, :two)
          batch.add_query(%<INSERT INTO things (a, b) VALUES (?, ?)>, ['foo', 3], [:varchar, :int])
          bytes = batch.write(CqlByteBuffer.new, 2, encoder)
          bytes.to_s[56, 8].should == "\x00\x00\x00\x04\x00\x00\x00\x03"
        end
      end

      describe '#to_s' do
        context 'when the type is LOGGED' do
          it 'returns a string representation of the batch request' do
            batch = described_class.new(described_class::LOGGED_TYPE, :local_quorum)
            batch.add_prepared(statement_id, ['arg', 3], [:varchar, :int])
            batch.add_query(%<INSERT INTO things (a, b) VALUES ('arg', 3)>, [], [:varchar, :bigint])
            batch.to_s.should == %(BATCH LOGGED 2 LOCAL_QUORUM)
          end
        end

        context 'when the type is UNLOGGED' do
          it 'returns a string representation of the batch request' do
            batch = described_class.new(described_class::UNLOGGED_TYPE, :one)
            batch.to_s.should == %(BATCH UNLOGGED 0 ONE)
          end
        end

        context 'when the type is COUNTER' do
          it 'returns a string representation of the batch request' do
            batch = described_class.new(described_class::COUNTER_TYPE, :two)
            batch.to_s.should == %(BATCH COUNTER 0 TWO)
          end
        end
      end
    end
  end
end
