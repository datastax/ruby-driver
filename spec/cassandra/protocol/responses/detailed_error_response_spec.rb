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
    describe DetailedErrorResponse do
      describe '.decode' do
        it 'decodes an unavailable error' do
          buffer = CqlByteBuffer.new("\x00\x05\x00\x00\x00\x03\x00\x00\x00\x02")
          response = described_class.decode(0x1000, '', 1, buffer, buffer.length)
          response.details.should == {
            :cl => :all,
            :required => 3,
            :alive => 2
          }
        end

        it 'decodes a write_timeout error' do
          buffer = CqlByteBuffer.new("\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00\tBATCH_LOG")
          response = described_class.decode(0x1100, '', 1, buffer, buffer.length)
          response.details.should == {
            :cl => :one,
            :received => 0,
            :blockfor => 1,
            :write_type => :batch_log
          }
        end

        it 'decodes a read_timeout error' do
          buffer = CqlByteBuffer.new("\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x01")
          response = described_class.decode(0x1200, '', 1, buffer, buffer.length)
          response.details.should == {
            :cl => :one,
            :received => 0,
            :blockfor => 1,
            :data_present => true
          }
          buffer = CqlByteBuffer.new("\x00\x01\x00\x00\x00\x00\x00\x00\x00\x01\x00")
          response = described_class.decode(0x1200, '', 1, buffer, buffer.length)
          response.details.should == {
            :cl => :one,
            :received => 0,
            :blockfor => 1,
            :data_present => false
          }
        end

        it 'decodes an already_exists error with a keyspace' do
          buffer = CqlByteBuffer.new("\x00\x05stuff\x00\x00")
          response = described_class.decode(0x2400, '', 1, buffer, buffer.length)
          response.details.should == {
            :ks => 'stuff',
            :table => '',
          }
        end

        it 'decodes an already_exists error with a keyspace and table' do
          buffer = CqlByteBuffer.new("\x00\x05stuff\x00\x06things")
          response = described_class.decode(0x2400, '', 1, buffer, buffer.length)
          response.details.should == {
            :ks => 'stuff',
            :table => 'things',
          }
        end

        it 'decodes unprepared error' do
          buffer = CqlByteBuffer.new("\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/")
          response = described_class.decode(0x2500, '', 1, buffer, buffer.length)
          response.details.should == {
            :id => "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
          }
        end
      end

      describe '#to_s' do
        it 'returns a string with the error code, message and details' do
          response = described_class.new(0xffff, 'This is an error', {:foo => 'bar'})
          response.to_s.should == 'ERROR 0xFFFF "This is an error" {:foo=>"bar"}'
        end
      end
    end
  end
end
