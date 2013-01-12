# encoding: ascii-8bit

require 'spec_helper'


module Cql
  describe ResponseFrame do
    let :frame do
      described_class.new
    end

    context 'when fed no data' do
      it 'has no length' do
        frame.length.should be_nil
      end

      it 'is not complete' do
        frame.should_not be_complete
      end
    end

    context 'when fed a header' do
      before do
        frame << "\x81\x00\x00\x02\x00\x00\x00\x16"
      end

      it 'knows the frame body length' do
        frame.length.should == 22
      end
    end

    context 'when fed a header in pieces' do
      before do
        frame << "\x81\x00"
        frame << "\x00\x02\x00\x00\x00"
        frame << "\x16"
      end

      it 'knows the body length' do
        frame.length.should == 22
      end
    end

    context 'when fed a request frame header' do
      it 'raises an UnsupportedFrameTypeError' do
        expect { frame << "\x01\x00\x00\x00\x00\x00\x00\x00" }.to raise_error(UnsupportedFrameTypeError)
      end
    end

    context 'when fed a header and a partial body' do
      before do
        frame << "\x81\x00"
        frame << "\x00\x06"
        frame << "\x00\x00\x00\x16"
        frame << [rand(255), rand(255), rand(255), rand(255), rand(255), rand(255), rand(255), rand(255)].pack('C')
      end

      it 'knows the body length' do
        frame.length.should == 22
      end

      it 'is not complete' do
        frame.should_not be_complete
      end
    end

    context 'when fed a complete ERROR frame' do
      before do
        frame << "\x81\x00\x00\x00\x00\x00\x00V\x00\x00\x00\n\x00PProvided version 4.0.0 is not supported by this server (supported: 2.0.0, 3.0.0)"
      end

      it 'is complete' do
        frame.should be_complete
      end

      it 'has an error code' do
        frame.body.code.should == 10
      end

      it 'has an error message' do
        frame.body.message.should == 'Provided version 4.0.0 is not supported by this server (supported: 2.0.0, 3.0.0)'
      end
    end

    context 'when fed a complete READY frame' do
      before do
        frame << [0x81, 0, 0, 0x02, 0].pack('C4N')
      end

      it 'is complete' do
        frame.should be_complete
      end
    end

    context 'when fed a complete SUPPORTED frame' do
      before do
        frame << "\x81\x00\x00\x06\x00\x00\x00\x27"
        frame << "\x00\x02\x00\x0bCQL_VERSION\x00\x01\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x00"
      end

      it 'is complete' do
        frame.should be_complete
      end

      it 'has options' do
        frame.body.options.should == {'CQL_VERSION' => ['3.0.0'], 'COMPRESSION' => []}
      end
    end

    context 'when fed a complete RESULT frame' do
      context 'when it\'s a set_keyspace' do
        before do
          frame << "\x81\x00\x00\b\x00\x00\x00\f"
          frame << "\x00\x00\x00\x03\x00\x06system"
        end

        it 'has a keyspace' do
          frame.body.keyspace.should == 'system'
        end
      end

      context 'when it\'s a schema_change' do
        context 'when it\'s a keyspace change' do
          before do
            frame << "\x81\x00\x00\b\x00\x00\x00\e\x00\x00\x00\x05\x00\aCREATED\x00\ncql_rb_477\x00\x00"
          end

          it 'has a change description' do
            frame.body.change.should == 'CREATED'
          end

          it 'has a keyspace' do
            frame.body.keyspace.should == 'cql_rb_477'
          end

          it 'has no table' do
            frame.body.table.should be_empty
          end
        end

        context 'when it\'s a table change' do
          before do
            frame << "\x81\x00\x00\b\x00\x00\x00 \x00\x00\x00\x05\x00\aUPDATED\x00\ncql_rb_973\x00\x05users"
          end

          it 'has a change description' do
            frame.body.change.should == 'UPDATED'
          end

          it 'has a keyspace' do
            frame.body.keyspace.should == 'cql_rb_973'
          end

          it 'has a table' do
            frame.body.table.should == 'users'
          end
        end
      end

      context 'when it\'s a void' do
        before do
          frame << "\x81\x00\x00\b\x00\x00\x00\x04\x00\x00\x00\x01"
        end

        it 'is has a body' do
          frame.body.should_not be_nil
        end
      end

      context 'when it\'s rows' do
        before do
          frame << "\x81\x00\x00\b\x00\x00\x00~\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\ncql_rb_126\x00\x05users\x00\tuser_name\x00\r\x00\x05email\x00\r\x00\bpassword\x00\r\x00\x00\x00\x02\x00\x00\x00\x04phil\x00\x00\x00\rphil@heck.com\xFF\xFF\xFF\xFF\x00\x00\x00\x03sue\x00\x00\x00\rsue@inter.net\xFF\xFF\xFF\xFF"
        end

        it 'has rows that are hashes of column name => column value' do
          frame.body.rows.should == [
            {'user_name' => 'phil', 'email' => 'phil@heck.com', 'password' => nil},
            {'user_name' => 'sue',  'email' => 'sue@inter.net', 'password' => nil}
          ]
        end
      end

      context 'when it\'s a prepared' do
        before do
          frame << "\x81\x00\x00\b\x00\x00\x00>"
          frame << "\x00\x00\x00\x04\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/\x00\x00\x00\x01\x00\x00\x00\x01\x00\ncql_rb_911\x00\x05users\x00\tuser_name\x00\r"
        end

        it 'has an id' do
          frame.body.id.should == "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
        end

        it 'has column metadata' do
          frame.body.metadata.should == [['cql_rb_911', 'users', 'user_name', :varchar]]
        end
      end

      context 'with different column types' do
        before do
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
          #   1358013521,
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

          frame << "\x81\x00\x00\b\x00\x00\x02K"
          frame << "\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x12\x00\x04test\x00\rlots_of_types\x00\fascii_column\x00\x01\x00\rbigint_column\x00\x02\x00\vblob_column\x00\x03\x00\x0Eboolean_column\x00\x04\x00\x0Edecimal_column\x00\x06\x00\rdouble_column\x00\a\x00\ffloat_column\x00\b\x00\vinet_column\x00\x10\x00\nint_column\x00\t\x00\vlist_column\x00 \x00\x01\x00\nmap_column\x00!\x00\r\x00\x04\x00\nset_column\x00\"\x00\x03\x00\vtext_column\x00\r\x00\x10timestamp_column\x00\v\x00\x0Ftimeuuid_column\x00\x0F\x00\vuuid_column\x00\f\x00\x0Evarchar_column\x00\r\x00\rvarint_column\x00\x0E\x00\x00\x00\x01\x00\x00\x00\x05hello\x00\x00\x00\b\x00\x03\x98\xB1S\xC8\x7F\xAB\x00\x00\x00\x05\xFA\xB4^4V\x00\x00\x00\x01\x01\x00\x00\x00\x11\x00\x00\x00\x12\r'\xFDI\xAD\x80f\x11g\xDCfV\xAA\x00\x00\x00\b@\xC3\x88\x0F\xC2\x7F\x9DU\x00\x00\x00\x04AB\x14{\x00\x00\x00\x04\n\x00\x01\x02\x00\x00\x00\x04\x00\xBCj\xC2\x00\x00\x00\x11\x00\x03\x00\x03foo\x00\x03foo\x00\x03bar\x00\x00\x00\x14\x00\x02\x00\x03foo\x00\x01\x01\x00\x05hello\x00\x01\x00\x00\x00\x00\r\x00\x02\x00\x03\xABC!\x00\x04\xAF\xD8~\xCD\x00\x00\x00\vhello world\x00\x00\x00\b\x00\x00\x00\x00P\xF1\xA4Q\x00\x00\x00\x10\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11\x00\x00\x00\x10\xCF\xD6l\xCC\xD8WN\x90\xB1\xE5\xDF\x98\xA3\xD4\f\xD6\x00\x00\x00\x03foo\x00\x00\x00\x11\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a["
        end

        it 'decodes ASCII as an ASCII encoded string' do
          frame.body.rows.first['ascii_column'].should == 'hello'
          frame.body.rows.first['ascii_column'].encoding.should == ::Encoding::ASCII
        end

        it 'decodes BIGINT as a number' do
          frame.body.rows.first['bigint_column'].should == 1012312312414123
        end

        it 'decodes BLOB as a ASCII-8BIT string' do
          frame.body.rows.first['blob_column'].should == "\xfa\xb4\x5e\x34\x56"
          frame.body.rows.first['blob_column'].encoding.should == ::Encoding::BINARY
        end

        it 'decodes BOOLEAN as a boolean' do
          frame.body.rows.first['boolean_column'].should equal(true)
        end

        it 'decodes DECIMAL as a number' do
          frame.body.rows.first['decimal_column'].should == BigDecimal.new(1042342234234123423435647768234, 18)
        end

        it 'decodes DOUBLE as a number' do
          frame.body.rows.first['double_column'].should == 10000.123123123
        end

        it 'decodes FLOAT as a number' do
          frame.body.rows.first['float_column'].should be_within(0.001).of(12.13)
        end

        it 'decodes INT as a number' do
          frame.body.rows.first['int_column'].should == 12348098
        end

        it 'decodes TEXT as a UTF-8 encoded string' do
          frame.body.rows.first['text_column'].should == 'hello world'
          frame.body.rows.first['text_column'].encoding.should == ::Encoding::UTF_8
        end

        it 'decodes TIMESTAMP as a Time' do
          frame.body.rows.first['timestamp_column'].should == Time.at(1358013521)
        end

        it 'decodes UUID as a Uuid' do
          frame.body.rows.first['uuid_column'].should == Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6')
        end

        it 'decodes VARCHAR as a UTF-8 encoded string' do
          frame.body.rows.first['varchar_column'].should == 'foo'
          frame.body.rows.first['varchar_column'].encoding.should == ::Encoding::UTF_8
        end

        it 'decodes VARINT as a number' do
          frame.body.rows.first['varint_column'].should == 1231312312331283012830129382342342412123
        end

        it 'decodes TIMEUUID as a Uuid' do
          frame.body.rows.first['timeuuid_column'].should == Uuid.new('a4a70900-24e1-11df-8924-001ff3591711')
        end

        it 'decodes INET as a IPAddr' do
          frame.body.rows.first['inet_column'].should == IPAddr.new('10.0.1.2')
        end

        it 'decodes LIST<ASCII> as an array of ASCII strings' do
          frame.body.rows.first['list_column'].should == ['foo', 'foo', 'bar'].map { |s| s.force_encoding(::Encoding::ASCII) }
        end

        it 'decodes MAP<TEXT, BOOLEAN> as a hash of UTF-8 strings to booleans' do
          frame.body.rows.first['map_column'].should == {'foo' => true, 'hello' => false}
        end

        it 'decodes SET<BLOB> as a set of binary strings' do
          frame.body.rows.first['set_column'].should == Set.new(["\xab\x43\x21", "\xaf\xd8\x7e\xcd"].map { |s| s.force_encoding(::Encoding::BINARY) })
        end

        it 'raises an error when encountering an unknown column type' do
          frame = described_class.new
          frame << "\x81\x00\x00\b\x00\x00\x00E"
          frame << "\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\ncql_rb_328\x00\x05users"
          expect { frame << "\x00\tuser_name\x00\xff\x00\x05email\x00\r\x00\bpassword\x00\r\x00\x00\x00\x00" }.to raise_error(UnsupportedColumnTypeError)
        end
      end

      context 'when it\'s an unknown type' do
        it 'raises an error' do
          expect { frame << "\x81\x00\x00\b\x00\x00\x00\x05\x00\x00\x00\xffhello" }.to raise_error(UnsupportedResultKindError)
        end
      end
    end

    context 'when fed an non-existent opcode' do
      it 'raises an UnsupportedOperationError' do
        expect { frame << "\x81\x00\x00\x99\x00\x00\x00\x39" }.to raise_error(UnsupportedOperationError)
      end
    end
  end
end