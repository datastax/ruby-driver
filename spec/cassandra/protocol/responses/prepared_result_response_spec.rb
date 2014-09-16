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
      describe '.decode' do
        context 'with a protocol v1 frame' do
          let :response do
            buffer = CqlByteBuffer.new
            buffer << "\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/" # statement ID
            buffer << "\x00\x00\x00\x01" # flags (global_tables_spec)
            buffer << "\x00\x00\x00\x01" # column count
            buffer << "\x00\ncql_rb_911\x00\x05users" # global_tables_spec
            buffer << "\x00\tuser_name\x00\r" # col_spec (name + type)
            described_class.decode(1, buffer, buffer.length)
          end

          it 'decodes the ID' do
            response.id.should == "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
          end

          it 'decodes the column metadata' do
            response.metadata.should == [['cql_rb_911', 'users', 'user_name', :varchar]]
          end
        end

        context 'with a protocol v2 frame' do
          let :response do
            buffer = CqlByteBuffer.new
            buffer << "\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/" # statement ID
            buffer << "\x00\x00\x00\x01" # flags (global_tables_spec)
            buffer << "\x00\x00\x00\x01" # column count
            buffer << "\x00\ncql_rb_911\x00\x05users" # global_tables_spec
            buffer << "\x00\tuser_name\x00\r" # col_spec (name + type)
            buffer << "\x00\x00\x00\x01" # flags (global_tables_spec)
            buffer << "\x00\x00\x00\x02" # column count
            buffer << "\x00\ncql_rb_911\x00\x05users" # global_tables_spec
            buffer << "\x00\tuser_name\x00\r" # col_spec (name + type)
            buffer << "\x00\x05email\x00\r" # col_spec (name + type)
            described_class.decode(2, buffer, buffer.length)
          end

          it 'decodes the ID' do
            response.id.should == "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
          end

          it 'decodes the column metadata' do
            response.metadata.should == [
              ['cql_rb_911', 'users', 'user_name', :varchar]
            ]
          end

          it 'decodes the result metadata' do
            response.result_metadata.should == [
              ['cql_rb_911', 'users', 'user_name', :varchar],
              ['cql_rb_911', 'users', 'email', :varchar]
            ]
          end

          it 'decodes the absence of result metadata' do
            buffer = CqlByteBuffer.new
            buffer << "\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/" # statement ID
            buffer << "\x00\x00\x00\x01" # flags (global_tables_spec)
            buffer << "\x00\x00\x00\x01" # column count
            buffer << "\x00\ncql_rb_911\x00\x05users" # global_tables_spec
            buffer << "\x00\tuser_name\x00\r" # col_spec (name + type)
            buffer << "\x00\x00\x00\x04" # flags (no_metadata)
            buffer << "\x00\x00\x00\x00" # column count
            response = described_class.decode(2, buffer, buffer.length)
            response.result_metadata.should be_nil
          end
        end

        context 'with user defined types' do
          let :buffer do
            b = CqlByteBuffer.new
            b << "\x00\x10\xC8\x90\r\x98\x06t\x97\x96\x94\xDA6\x13\xBB\x9D\xA5\xE1" # statement ID
            b << "\x00\x00\x00\x00" # flags
            b << "\x00\x00\x00\x00" # column count
            b << "\x00\x00\x00\x01" # flags (global_tables_spec)
            b << "\x00\x00\x00\x03" # column count
            b << "\x00\x12user_defined_types\x00\x05users" # global_tables_spec
            b << "\x00\x02id\x00\f" # col_spec (name + type)
            b << "\x00\taddresses\x00!\x00\r\x00\x00\x01Morg.apache.cassandra.db.marshal.UserType(user_defined_types,61646472657373,737472656574:org.apache.cassandra.db.marshal.UTF8Type,63697479:org.apache.cassandra.db.marshal.UTF8Type,7a69705f636f6465:org.apache.cassandra.db.marshal.Int32Type,70686f6e6573:org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type))" # col_spec (name + type + extra type info)
            b << "\x00\x04name\x00\r"
          end

          it 'decodes the full type hierarchy' do
            response = described_class.decode(2, buffer, buffer.length)
            column_metadata = response.result_metadata[1]
            type_description = column_metadata[3]
            type_description.should == [:map, :varchar, [:udt, {'street' => :text, 'city' => :text, 'zip_code' => :int, 'phones' => [:set, :text]}]]
          end
        end
      end

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
