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
    describe PreparedResultResponse do
      describe '#void?' do
        it 'is not void' do
          response = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          response.should_not be_void
        end
      end

      describe '#to_s' do
        it 'returns a string with the ID and metadata' do
          response = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\x00/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          response.to_s.should match(/^RESULT PREPARED [0-9a-f]{32} \[\["ks", "tbl", "col", :varchar\]\]$/)
        end
      end

      describe '#eql?' do
        it 'is equal to an identical response' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r1.should eql(r2)
        end

        it 'is not equal when the IDs differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r2 = described_class.new("\x00" * 16, [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r1.should_not eql(r2)
        end

        it 'is not equal when the metadata differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar], ['ks', 'tbl', 'col2', :uuid]], nil, nil)
          r1.should_not eql(r2)
        end

        it 'is not equal when one has a trace ID' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r1.should_not eql(r2)
        end

        it 'is not equal when the trace IDs differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, Uuid.new('11111111-d0e1-11e2-8b8b-0800200c9a66'))
          r1.should_not eql(r2)
        end

        it 'is aliased as ==' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r1.should == r2
        end
      end

      describe '#hash' do
        it 'is the same for an identical response' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r1.hash.should == r2.hash
        end

        it 'is not the same when the IDs differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r2 = described_class.new("\x00" * 16, [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r1.hash.should_not == r2.hash
        end

        it 'is not the same when the metadata differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar], ['ks', 'tbl', 'col2', :uuid]], nil, nil)
          r1.hash.should_not == r2.hash
        end

        it 'is not the same when one has a trace ID' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, nil)
          r1.hash.should_not == r2.hash
        end

        it 'is not equal when the trace IDs differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil, Uuid.new('11111111-d0e1-11e2-8b8b-0800200c9a66'))
          r1.hash.should_not == r2.hash
        end
      end
    end
  end
end
