# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe ExecuteRequest do
      let :id do
        "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
      end

      let :column_metadata do
        [
          ['ks', 'tbl', 'col1', :varchar],
          ['ks', 'tbl', 'col2', :int],
          ['ks', 'tbl', 'col3', :varchar]
        ]
      end

      let :values do
        ['hello', 42, 'foo']
      end

      describe '#initialize' do
        it 'raises an error when the metadata and values don\'t have the same size' do
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42], true, :each_quorum, nil, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the consistency is nil' do
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, nil, nil, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the consistency is invalid' do
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :hello, nil, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the serial consistency is invalid' do
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :quorum, :foo, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when paging state is given but no page size' do
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :quorum, nil, nil, 'foo', false) }.to raise_error(ArgumentError)
        end

        it 'raises an error for unsupported column types' do
          column_metadata[2][3] = :imaginary
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :each_quorum, nil, nil, nil, false) }.to raise_error(UnsupportedColumnTypeError)
        end

        it 'raises an error for unsupported column collection types' do
          column_metadata[2][3] = [:imaginary, :varchar]
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42, ['foo']], true, :each_quorum, nil, nil, nil, false) }.to raise_error(UnsupportedColumnTypeError)
        end

        it 'raises an error when collection values are not enumerable' do
          column_metadata[2][3] = [:set, :varchar]
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :each_quorum, nil, nil, nil, false) }.to raise_error(InvalidValueError)
        end

        it 'raises an error when it cannot encode the argument' do
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 'not an int', 'foo'], true, :each_quorum, nil, nil, nil, false) }.to raise_error(TypeError, /cannot be encoded as INT/)
        end
      end

      describe '#write' do
        context 'when the protocol version is 1' do
          let :frame_bytes do
            ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :each_quorum, nil, nil, nil, false).write(1, CqlByteBuffer.new)
          end

          it 'writes the statement ID' do
            frame_bytes.to_s[0, 18].should == "\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
          end

          it 'writes the number of bound variables' do
            frame_bytes.to_s[18, 2].should == "\x00\x03"
          end

          it 'writes the bound variables' do
            frame_bytes.to_s[20, 24].should == "\x00\x00\x00\x05hello\x00\x00\x00\x04\x00\x00\x00\x2a\x00\x00\x00\x03foo"
          end

          it 'writes the consistency' do
            frame_bytes.to_s[44, 999].should == "\x00\x07"
          end
        end

        context 'when the protocol version is 2' do
          let :frame_bytes do
            ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :each_quorum, nil, nil, nil, false).write(2, CqlByteBuffer.new)
          end

          it 'writes the statement ID' do
            frame_bytes.to_s[0, 18].should == "\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
          end

          it 'writes the consistency' do
            frame_bytes.to_s[18, 2].should == "\x00\x07"
          end

          it 'writes flags saying that there will be bound values values' do
            frame_bytes.to_s[20, 1].should == "\x01"
          end

          it 'does not write the bound values flag when there are no values, and does not write anything more' do
            frame_bytes = ExecuteRequest.new(id, [], [], true, :each_quorum, nil, nil, nil, false).write(2, CqlByteBuffer.new)
            frame_bytes.to_s[20, 999].should == "\x00"
          end

          it 'writes flags saying that the result doesn\'t need to contain metadata' do
            frame_bytes = ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], false, :each_quorum, nil, nil, nil, false).write(2, CqlByteBuffer.new)
            frame_bytes.to_s[20, 1].should == "\x03"
          end

          it 'writes the number of bound values' do
            frame_bytes.to_s[21, 2].should == "\x00\x03"
          end

          it 'writes the bound values' do
            frame_bytes.to_s[23, 999].should == "\x00\x00\x00\x05hello\x00\x00\x00\x04\x00\x00\x00\x2a\x00\x00\x00\x03foo"
          end

          it 'sets the serial flag and includes the serial consistency' do
            frame_bytes = ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], false, :each_quorum, :local_serial, false).write(2, CqlByteBuffer.new)
            frame_bytes.to_s[20, 1].should == "\x13"
            frame_bytes.to_s[47, 2].should == "\x00\x09"
          end

          it 'writes the page size flag and page size' do
            frame_bytes = ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :one, nil, 10, nil, false).write(2, CqlByteBuffer.new)
            (frame_bytes.to_s[20, 1].ord & 0x04).should == 0x04
            frame_bytes.to_s[47, 4].should == "\x00\x00\x00\x0a"
          end

          it 'writes the page size and paging state flag and the page size and paging state' do
            frame_bytes = ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], true, :one, nil, 10, 'foobar', false).write(2, CqlByteBuffer.new)
            (frame_bytes.to_s[20, 1].ord & 0x0c).should == 0x0c
            frame_bytes.to_s[47, 4].should == "\x00\x00\x00\x0a"
            frame_bytes.to_s[51, 10].should == "\x00\x00\x00\x06foobar"
          end
        end

        context 'with different data types' do
          specs = [
            [:ascii, 'test', "test"],
            [:bigint, 1012312312414123, "\x00\x03\x98\xB1S\xC8\x7F\xAB"],
            [:blob, "\xab\xcd", "\xab\xcd"],
            [:boolean, false, "\x00"],
            [:boolean, true, "\x01"],
            [:decimal, BigDecimal.new('1042342234234.123423435647768234'), "\x00\x00\x00\x12\r'\xFDI\xAD\x80f\x11g\xDCfV\xAA"],
            [:double, 10000.123123123, "@\xC3\x88\x0F\xC2\x7F\x9DU"],
            [:float, 12.13, "AB\x14{"],
            [:inet, IPAddr.new('8.8.8.8'), "\x08\x08\x08\x08"],
            [:inet, IPAddr.new('::1'), "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"],
            [:int, 12348098, "\x00\xBCj\xC2"],
            [:text, 'FOOBAR', 'FOOBAR'],
            [:timestamp, Time.at(1358013521.123), "\x00\x00\x01</\xE9\xDC\xE3"],
            [:timeuuid, Uuid.new('a4a70900-24e1-11df-8924-001ff3591711'), "\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11"],
            [:uuid, Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6'), "\xCF\xD6l\xCC\xD8WN\x90\xB1\xE5\xDF\x98\xA3\xD4\f\xD6"],
            [:varchar, 'hello', 'hello'],
            [:varint, 1231312312331283012830129382342342412123, "\x03\x9EV \x15\f\x03\x9DK\x18\xCDI\\$?\a["],
            [:varint, -234234234234, "\xC9v\x8D:\x86"],
            [[:list, :timestamp], [Time.at(1358013521.123)], "\x00\x01" + "\x00\x08\x00\x00\x01</\xE9\xDC\xE3"],
            [[:list, :boolean], [true, false, true, true], "\x00\x04" + "\x00\x01\x01" + "\x00\x01\x00"  + "\x00\x01\x01" + "\x00\x01\x01"],
            [[:map, :uuid, :int], {Uuid.new('cfd66ccc-d857-4e90-b1e5-df98a3d40cd6') => 45345, Uuid.new('a4a70900-24e1-11df-8924-001ff3591711') => 98765}, "\x00\x02" + "\x00\x10\xCF\xD6l\xCC\xD8WN\x90\xB1\xE5\xDF\x98\xA3\xD4\f\xD6" + "\x00\x04\x00\x00\xb1\x21" + "\x00\x10\xA4\xA7\t\x00$\xE1\x11\xDF\x89$\x00\x1F\xF3Y\x17\x11" + "\x00\x04\x00\x01\x81\xcd"],
            [[:map, :ascii, :blob], {'hello' => 'world', 'one' => "\x01", 'two' => "\x02"}, "\x00\x03" + "\x00\x05hello" + "\x00\x05world" + "\x00\x03one" + "\x00\x01\x01" + "\x00\x03two" + "\x00\x01\x02"],
            [[:set, :int], Set.new([13, 3453, 456456, 123, 768678]), "\x00\x05" + "\x00\x04\x00\x00\x00\x0d" + "\x00\x04\x00\x00\x0d\x7d" + "\x00\x04\x00\x06\xf7\x08" + "\x00\x04\x00\x00\x00\x7b" + "\x00\x04\x00\x0b\xba\xa6"],
            [[:set, :varchar], Set.new(['foo', 'bar', 'baz']), "\x00\x03" + "\x00\x03foo" + "\x00\x03bar" + "\x00\x03baz"],
            [[:set, :int], [13, 3453, 456456, 123, 768678], "\x00\x05" + "\x00\x04\x00\x00\x00\x0d" + "\x00\x04\x00\x00\x0d\x7d" + "\x00\x04\x00\x06\xf7\x08" + "\x00\x04\x00\x00\x00\x7b" + "\x00\x04\x00\x0b\xba\xa6"],
            [[:set, :varchar], ['foo', 'bar', 'baz'], "\x00\x03" + "\x00\x03foo" + "\x00\x03bar" + "\x00\x03baz"]
          ]
          specs.each do |type, value, expected_bytes|
            it "encodes #{type} values" do
              metadata = [['ks', 'tbl', 'id_column', type]]
              buffer = ExecuteRequest.new(id, metadata, [value], true, :one, nil, nil, nil, false).write(1, CqlByteBuffer.new)
              buffer.discard(2 + 16 + 2)
              length = buffer.read_int
              result_bytes = buffer.read(length)
              result_bytes.should eql_bytes(expected_bytes)
            end
          end
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = ExecuteRequest.new(id, column_metadata, values, true, :each_quorum, nil, nil, nil, false)
          request.to_s.should == 'EXECUTE ca487f1e7a82d23c4e8af3355171a52f ["hello", 42, "foo"] EACH_QUORUM'
        end
      end

      describe '#eql?' do
        it 'returns true when the ID, metadata, values and consistency are the same' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e1.should eql(e2)
        end

        it 'returns false when the ID is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id.reverse, column_metadata, values, true, :one, nil, nil, nil, false)
          e1.should_not eql(e2)
        end

        it 'returns false when the metadata is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata.reverse, values, true, :one, nil, nil, nil, false)
          e1.should_not eql(e2)
        end

        it 'returns false when the values are different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values.reverse, true, :one, nil, nil, nil, false)
          e1.should_not eql(e2)
        end

        it 'returns false when the consistency is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :two, nil, nil, nil, false)
          e1.should_not eql(e2)
        end

        it 'returns false when the serial consistency is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, :serial, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :one, :local_serial, nil, nil, false)
          e1.should_not eql(e2)
        end

        it 'returns false when the page size is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, 10, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, 20, nil, false)
          e1.should_not eql(e2)
        end

        it 'returns false when the paging state is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, 10, 'foo', false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, 10, 'bar', false)
          e1.should_not eql(e2)
        end

        it 'is aliased as ==' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e1.should == e2
        end
      end

      describe '#hash' do
        it 'has the same hash code as another identical object' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e1.hash.should == e2.hash
        end

        it 'does not have the same hash code when the ID is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id.reverse, column_metadata, values, true, :one, nil, nil, nil, false)
          e1.hash.should_not == e2.hash
        end

        it 'does not have the same hash code when the metadata is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata.reverse, values, true, :one, nil, nil, nil, false)
          e1.hash.should_not == e2.hash
        end

        it 'does not have the same hash code when the values are different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values.reverse, true, :one, nil, nil, nil, false)
          e1.hash.should_not == e2.hash
        end

        it 'does not have the same hash code when the consistency is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :two, nil, nil, nil, false)
          e1.hash.should_not == e2.hash
        end

        it 'does not have the same hash code when the serial consistency is different' do
          e1 = ExecuteRequest.new(id, column_metadata, values, true, :one, nil, nil, nil, false)
          e2 = ExecuteRequest.new(id, column_metadata, values, true, :one, :serial, nil, nil, false)
          e1.hash.should_not == e2.hash
        end
      end
    end
  end
end
