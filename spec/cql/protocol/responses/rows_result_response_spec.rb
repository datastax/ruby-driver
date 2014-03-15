# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe RowsResultResponse do
      describe '.decode' do
        context 'with rows from the same table' do
          let :response do
            buffer = CqlByteBuffer.new("\x00\x00\x00\x01\x00\x00\x00\x03\x00\ncql_rb_126\x00\x05users\x00\tuser_name\x00\r\x00\x05email\x00\r\x00\bpassword\x00\r\x00\x00\x00\x02\x00\x00\x00\x04phil\x00\x00\x00\rphil@heck.com\xFF\xFF\xFF\xFF\x00\x00\x00\x03sue\x00\x00\x00\rsue@inter.net\xFF\xFF\xFF\xFF")
            described_class.decode(1, buffer, buffer.length)
          end

          it 'decodes the rows as hashes of column name => column value' do
            response.rows.should == [
              {'user_name' => 'phil', 'email' => 'phil@heck.com', 'password' => nil},
              {'user_name' => 'sue',  'email' => 'sue@inter.net', 'password' => nil}
            ]
          end

          it 'decodes column metadata' do
            response.metadata.should == [
              ['cql_rb_126', 'users', 'user_name', :varchar],
              ['cql_rb_126', 'users', 'email', :varchar],
              ['cql_rb_126', 'users', 'password', :varchar]
            ]
          end

          it 'is not void' do
            response.should_not be_void
          end
        end

        context 'with rows from different keyspaces' do
          let :response do
            # TODO: not sure if this is really how it would be for real
            # this frame was constructed from the spec not from an actual result
            buffer = CqlByteBuffer.new
            buffer << "\x00\x00\x00\x00"
            buffer << "\x00\x00\x00\x03"
            buffer << "\x00\ncql_rb_126\x00\x06users1\x00\tuser_name\x00\r"
            buffer << "\x00\ncql_rb_127\x00\x06users2\x00\x05email\x00\r"
            buffer << "\x00\ncql_rb_128\x00\x06users3\x00\bpassword\x00\r"
            buffer << "\x00\x00\x00\x02\x00\x00\x00\x04phil\x00\x00\x00\rphil@heck.com\xFF\xFF\xFF\xFF\x00\x00\x00\x03sue\x00\x00\x00\rsue@inter.net\xFF\xFF\xFF\xFF"
            described_class.decode(1, buffer, buffer.length)
          end

          it 'decodes the rows' do
            response.rows.should == [
              {'user_name' => 'phil', 'email' => 'phil@heck.com', 'password' => nil},
              {'user_name' => 'sue',  'email' => 'sue@inter.net', 'password' => nil}
            ]
          end

          it 'decodes the column metadata' do
            response.metadata.should == [
              ['cql_rb_126', 'users1', 'user_name', :varchar],
              ['cql_rb_127', 'users2', 'email', :varchar],
              ['cql_rb_128', 'users3', 'password', :varchar]
            ]
          end
        end

        context 'when there is no metadata' do
          let :response do
            buffer = CqlByteBuffer.new
            buffer << "\x00\x00\x00\x05" # flags (global_tables_spec | no_metadata)
            buffer << "\x00\x00\x00\x03" # column count
            buffer << "\x00\x00\x00\x02\x00\x00\x00\x04phil\x00\x00\x00\rphil@heck.com\xFF\xFF\xFF\xFF\x00\x00\x00\x03sue\x00\x00\x00\rsue@inter.net\xFF\xFF\xFF\xFF"
            described_class.decode(2, buffer, buffer.length)
          end

          it 'returns a RawRowsResultResponse' do
            metadata = [
              ['cql_rb_126', 'users', 'user_name', :varchar],
              ['cql_rb_126', 'users', 'email', :varchar],
              ['cql_rb_126', 'users', 'password', :varchar]
            ]
            response.materialize(metadata)
            response.rows.should == [
              {'user_name' => 'phil', 'email' => 'phil@heck.com', 'password' => nil},
              {'user_name' => 'sue',  'email' => 'sue@inter.net', 'password' => nil}
            ]
          end
        end

        context 'when there are more pages' do
          let :response do
            buffer = CqlByteBuffer.new
            buffer << "\x00\x00\x00\x03" # flags (global_tables_spec | has_more_pages)
            buffer << "\x00\x00\x00\x03" # column count
            buffer << "\x00\x00\x00\x03foo" # paging state
            buffer << "\x00\ncql_rb_126\x00\x05users"
            buffer << "\x00\tuser_name\x00\r\x00\x05email\x00\r\x00\bpassword\x00\r"
            buffer << "\x00\x00\x00\x02\x00\x00\x00\x04phil\x00\x00\x00\rphil@heck.com\xFF\xFF\xFF\xFF\x00\x00\x00\x03sue\x00\x00\x00\rsue@inter.net\xFF\xFF\xFF\xFF"
            described_class.decode(2, buffer, buffer.length)
          end

          it 'extracts the paging state' do
            response.paging_state.should == 'foo'
          end
        end

        context 'with different column types' do
          let :response do
            # The following test was created by intercepting the frame for the
            # SELECT statement in this CQL exchange
            #
            # CREATE TABLE lots_of_types (
            #   ascii_column     ASCII,
            #   bigint_column    BIGINT,
            #   blob_column      BLOB,
            #   boolean_column   BOOLEAN,
            #   decimal_column   DECIMAL,
            #   double_column    DOUBLE,
            #   float_column     FLOAT,
            #   int_column       INT,
            #   text_column      TEXT,
            #   timestamp_column TIMESTAMP,
            #   uuid_column      UUID,
            #   varchar_column   VARCHAR,
            #   varint_column    VARINT,
            #   timeuuid_column  TIMEUUID,
            #   inet_column      INET,
            #   list_column      LIST<ASCII>,
            #   map_column       MAP<TEXT, BOOLEAN>,
            #   set_column       SET<BLOB>,
            #
            #   PRIMARY KEY (ascii_column)
            # );
            #
            # INSERT INTO lots_of_types (ascii_column, bigint_column, blob_column, boolean_column, decimal_column, double_column, float_column, int_column, text_column, timestamp_column, uuid_column, varchar_column, varint_column, timeuuid_column, inet_column, list_column, map_column, set_column)
            # VALUES (
            #   'hello',
            #   1012312312414123,
            #   'fab45e3456',
            #   true,
            #   1042342234234.123423435647768234,
            #   10000.123123123,
            #   12.13,
            #   12348098,
            #   'hello world',
            #   1358013521.123,
            #   cfd66ccc-d857-4e90-b1e5-df98a3d40cd6,
            #   'foo',
            #   1231312312331283012830129382342342412123,
            #   a4a70900-24e1-11df-8924-001ff3591711,
            #   167772418,
            #   ['foo', 'foo', 'bar'],
            #   {'foo': true, 'hello': false},
            #   {'ab4321', 'afd87ecd'}
            # );
            #
            # SELECT * FROM lots_of_types WHERE ascii_column = 'hello';
            buffer = CqlByteBuffer.new("\x00\x00\x00\x01\x00\x00\x00\x12\x00\x04test\x00\rlots_of_types\x00\fascii_column\x00\x01\x00\rbigint_column\x00\x02\x00\vblob_column\x00\x03\x00\x0Eboolean_column\x00\x04\x00\x0Edecimal_column\x00\x06\x00\rdouble_column\x00\a\x00\ffloat_column\x00\b\x00\vinet_column\x00\x10\x00\nint_column\x00\t\x00\vlist_column\x00 \x00\x01\x00\nmap_column\x00!\x00\r\x00\x04\x00\nset_column\x00\"\x00\x03\x00\vtext_column\x00\r\x00\x10timestamp_column\x00\v\x00\x0Ftimeuuid_column\x00\x0F\x00\vuuid_column\x00\f\x00\x0Evarchar_column\x00\r\x00\rvarint_column\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x05hello\x00\x00\x00\b\x00\x03\x98\xB1S\xC8\x7F\xAB\x00\x00\x00\x05\xFA\xB4^4V\x00\x00\x00\x01\x01\x00\x00\x00\x11\x00\x00\x00\x12\r'\xFDI\xAD\x80f\x11g\xDCfV\xAA\x00\x00\x00\b@\xC3\x88\x0F\xC2\x7F\x9DU\x00\x00\x00\x04AB\x14{\x00\x00\x00\x04\n\x00\x01\x02\x00\x00\x00\x04\x00\xBCj\xC2\x00\x00\x00\x11\x00\x03\x00\x03foo\x00\x03foo\x00\x03bar\x00\x00\x00\x14\x00\x02\x00\x03foo\x00\x01\x01\x00\x05hello\x00\x01\x00\x00\x00\x00\r\x00\x02\x00\x03\xABC!\x00\x04\xAF\xD8~\xCD\x00\x00\x00\vhello world\x00\x00\x00\b\x00\x00\x01</\xE9\xDC\xE3\x00\x00\x00\x10\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11\x00\x00\x00\x10\xCF\xD6l\xCC\xD8WN\x90\xB1\xE5\xDF\x98\xA3\xD4\f\xD6\x00\x00\x00\x03foo\x00\x00\x00\x11\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a[")
            described_class.decode(1, buffer, buffer.length)
          end

          it 'decodes ASCII as an ASCII encoded string' do
            response.rows.first['ascii_column'].should == 'hello'
            response.rows.first['ascii_column'].encoding.should == ::Encoding::ASCII
          end

          it 'decodes BIGINT as a number' do
            response.rows.first['bigint_column'].should == 1012312312414123
          end

          it 'decodes BLOB as a ASCII-8BIT string' do
            response.rows.first['blob_column'].should == "\xfa\xb4\x5e\x34\x56"
            response.rows.first['blob_column'].encoding.should == ::Encoding::BINARY
          end

          it 'decodes BOOLEAN as a boolean' do
            response.rows.first['boolean_column'].should equal(true)
          end

          it 'decodes DECIMAL as a number' do
            response.rows.first['decimal_column'].should == BigDecimal.new('1042342234234.123423435647768234')
          end

          it 'decodes DOUBLE as a number' do
            response.rows.first['double_column'].should == 10000.123123123
          end

          it 'decodes FLOAT as a number' do
            response.rows.first['float_column'].should be_within(0.001).of(12.13)
          end

          it 'decodes INT as a number' do
            response.rows.first['int_column'].should == 12348098
          end

          it 'decodes TEXT as a UTF-8 encoded string' do
            response.rows.first['text_column'].should == 'hello world'
            response.rows.first['text_column'].encoding.should == ::Encoding::UTF_8
          end

          it 'decodes TIMESTAMP as a Time' do
            response.rows.first['timestamp_column'].should == Time.at(1358013521.123)
          end

          it 'decodes UUID as a Uuid' do
            response.rows.first['uuid_column'].should == Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6')
          end

          it 'decodes VARCHAR as a UTF-8 encoded string' do
            response.rows.first['varchar_column'].should == 'foo'
            response.rows.first['varchar_column'].encoding.should == ::Encoding::UTF_8
          end

          it 'decodes VARINT as a number' do
            response.rows.first['varint_column'].should == 1231312312331283012830129382342342412123
          end

          it 'decodes TIMEUUID as a TimeUuid' do
            response.rows.first['timeuuid_column'].should == TimeUuid.new('a4a70900-24e1-11df-8924-001ff3591711')
          end

          it 'decodes INET as a IPAddr' do
            response.rows.first['inet_column'].should == IPAddr.new('10.0.1.2')
          end

          it 'decodes LIST<ASCII> as an array of ASCII strings' do
            response.rows.first['list_column'].should == ['foo', 'foo', 'bar'].map { |s| s.force_encoding(::Encoding::ASCII) }
          end

          it 'decodes MAP<TEXT, BOOLEAN> as a hash of UTF-8 strings to booleans' do
            response.rows.first['map_column'].should == {'foo' => true, 'hello' => false}
          end

          it 'decodes SET<BLOB> as a set of binary strings' do
            response.rows.first['set_column'].should == Set.new(["\xab\x43\x21", "\xaf\xd8\x7e\xcd"].map { |s| s.force_encoding(::Encoding::BINARY) })
          end
        end

        context 'with null values' do
          it 'decodes nulls' do
            buffer = CqlByteBuffer.new("\x00\x00\x00\x01\x00\x00\x00\x13\x00\x12cql_rb_client_spec\x00\rlots_of_types\x00\x02id\x00\t\x00\fascii_column\x00\x01\x00\rbigint_column\x00\x02\x00\vblob_column\x00\x03\x00\x0Eboolean_column\x00\x04\x00\x0Edecimal_column\x00\x06\x00\rdouble_column\x00\a\x00\ffloat_column\x00\b\x00\vinet_column\x00\x10\x00\nint_column\x00\t\x00\vlist_column\x00 \x00\x01\x00\nmap_column\x00!\x00\r\x00\x04\x00\nset_column\x00\"\x00\x03\x00\vtext_column\x00\r\x00\x10timestamp_column\x00\v\x00\x0Ftimeuuid_column\x00\x0F\x00\vuuid_column\x00\f\x00\x0Evarchar_column\x00\r\x00\rvarint_column\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x04\x00\x00\x00\x03\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")
            response = described_class.decode(1, buffer, buffer.length)
            response.rows.first.should eql(
              'id' => 3,
              'ascii_column' => nil,
              'bigint_column' => nil,
              'blob_column' => nil,
              'boolean_column' => nil,
              'decimal_column' => nil,
              'double_column' => nil,
              'float_column' => nil,
              'int_column' => nil,
              'text_column' => nil,
              'timestamp_column' => nil,
              'uuid_column' => nil,
              'varchar_column' => nil,
              'varint_column' => nil,
              'timeuuid_column' => nil,
              'inet_column' => nil,
              'list_column' => nil,
              'map_column' => nil,
              'set_column' => nil,
            )
          end
        end

        context 'with COUNTER columns' do
          it 'decodes COUNTER as a number' do
            buffer = CqlByteBuffer.new("\x00\x00\x00\x01\x00\x00\x00\x03\x00\x04test\x00\x04cnts\x00\x02id\x00\r\x00\x02c1\x00\x05\x00\x02c2\x00\x05\x00\x00\x00\x01\x00\x00\x00\x04theo\x00\x00\x00\b\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\b\x00\x00\x00\x00\x00\x00\x00\x01")
            response = described_class.decode(1, buffer, buffer.length)
            response.rows.first['c1'].should == 3
          end

          it 'decodes a null COUNTER as nil' do
            buffer = CqlByteBuffer.new("\x00\x00\x00\x01\x00\x00\x00\x02\x00\x12cql_rb_client_spec\x00\bcounters\x00\bcounter1\x00\x05\x00\bcounter2\x00\x05\x00\x00\x00\x01\x00\x00\x00\b\x00\x00\x00\x00\x00\x00\x00\x01\xFF\xFF\xFF\xFF")
            response = described_class.decode(1, buffer, buffer.length)
            response.rows.first['counter2'].should be_nil
          end
        end

        context 'with an INET column' do
          let :response do
            buffer = CqlByteBuffer.new("\x00\x00\x00\x01\x00\x00\x00\x01\x00\ntest_types\x00\rlots_of_types\x00\vinet_column\x00\x10\x00\x00\x00\x02\x00\x00\x00\x04\x7F\x00\x00\x01\x00\x00\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01")
            described_class.decode(1, buffer, buffer.length)
          end

          it 'decodes IPv4 values' do
            response.rows[0]['inet_column'].should == IPAddr.new('127.0.0.1')
          end

          it 'decodes IPv6 values' do
            response.rows[1]['inet_column'].should == IPAddr.new('::1')
          end
        end

        context 'with an unknown column type' do
          it 'raises an error when encountering an unknown column type' do
            buffer = CqlByteBuffer.new("\x00\x00\x00\x01\x00\x00\x00\x03\x00\ncql_rb_328\x00\x05users\x00\tuser_name\x00\xff\x00\x05email\x00\r\x00\bpassword\x00\r\x00\x00\x00\x00")
            expect { described_class.decode(1, buffer, buffer.length) }.to raise_error(UnsupportedColumnTypeError)
          end
        end
      end

      describe '#void?' do
        it 'is not void' do
          response = RowsResultResponse.new([{'col' => 'foo'}], [['ks', 'tbl', 'col', :varchar]], nil, nil)
          response.should_not be_void
        end
      end

      describe '#to_s' do
        it 'returns a string with metadata and rows' do
          response = RowsResultResponse.new([{'col' => 'foo'}], [['ks', 'tbl', 'col', :varchar]], nil, nil)
          response.to_s.should == 'RESULT ROWS [["ks", "tbl", "col", :varchar]] [{"col"=>"foo"}]'
        end
      end
    end
  end
end
