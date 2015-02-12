# encoding: ascii-8bit

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
  module Protocol
    describe RawRowsResultResponse do
      let :response do
        described_class.new(2, raw_rows, nil, nil)
      end

      let :raw_rows do
        CqlByteBuffer.new("\x00\x00\x00\x02\x00\x00\x00\x04phil\x00\x00\x00\x0dphil@heck.com\xff\xff\xff\xff\x00\x00\x00\x03sue\x00\x00\x00\x0dsue@inter.net\xff\xff\xff\xff")
      end

      let :metadata do
        [
          ['ks', 'users', 'user_name', Types.varchar],
          ['ks', 'users', 'email', Types.varchar],
          ['ks', 'users', 'password', Types.varchar],
        ]
      end

      describe '#materialize' do
        it 'decodes the rows using the given metadata' do
          response.materialize(metadata)
          response.rows.should == [
            {'user_name' => 'phil', 'email' => 'phil@heck.com', 'password' => nil},
            {'user_name' => 'sue', 'email' => 'sue@inter.net', 'password' => nil},
          ]
        end

        it 'returns the rows' do
          rows = response.materialize(metadata)
          rows.should == [
            {'user_name' => 'phil', 'email' => 'phil@heck.com', 'password' => nil},
            {'user_name' => 'sue', 'email' => 'sue@inter.net', 'password' => nil},
          ]
        end
      end

      describe '#rows' do
        it 'raises an error before #materialize has been called' do
          expect { response.rows }.to raise_error(Errors::DecodingError)
          response.materialize(metadata)
          response.rows
        end
      end

      describe '#metadata' do
        it 'returns nil before #materialize has been called' do
          response.metadata.should be_nil
          response.materialize(metadata)
          response.metadata.should == metadata
        end
      end

      describe '#to_s' do
        it 'returns a static string' do
          response.to_s.should == 'RESULT ROWS (raw)'
        end
      end
    end
  end
end
