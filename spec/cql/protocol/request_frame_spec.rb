# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe RequestFrame do
      context 'with CREDENTIALS requests' do
        it 'encodes a CREDENTIALS request' do
          bytes = RequestFrame.new(CredentialsRequest.new('username' => 'cassandra', 'password' => 'ardnassac')).write('')
          bytes.should == (
            "\x01\x00\x00\04" +
            "\x00\x00\x00\x2c" +
            "\x00\x02" +
            "\x00\x08username" +
            "\x00\x09cassandra" +
            "\x00\x08password" +
            "\x00\x09ardnassac"
          )
        end
      end

      context 'with OPTIONS requests' do
        it 'encodes an OPTIONS request' do
          bytes = RequestFrame.new(OptionsRequest.new).write('')
          bytes.should == "\x01\x00\x00\x05\x00\x00\x00\x00"
        end
      end

      context 'with STARTUP requests' do
        it 'encodes the request' do
          bytes = RequestFrame.new(StartupRequest.new('3.0.0', 'snappy')).write('')
          bytes.should == "\x01\x00\x00\x01\x00\x00\x00\x2b\x00\x02\x00\x0bCQL_VERSION\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x06snappy"
        end

        it 'defaults to CQL 3.0.0 and no compression' do
          bytes = RequestFrame.new(StartupRequest.new).write('')
          bytes.should == "\x01\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0bCQL_VERSION\x00\x053.0.0"
        end
      end

      context 'with REGISTER requests' do
        it 'encodes the request' do
          bytes = RequestFrame.new(RegisterRequest.new('TOPOLOGY_CHANGE', 'STATUS_CHANGE')).write('')
          bytes.should == "\x01\x00\x00\x0b\x00\x00\x00\x22\x00\x02\x00\x0fTOPOLOGY_CHANGE\x00\x0dSTATUS_CHANGE"
        end
      end

      context 'with QUERY requests' do
        it 'encodes the request' do
          bytes = RequestFrame.new(QueryRequest.new('USE system', :all)).write('')
          bytes.should == "\x01\x00\x00\x07\x00\x00\x00\x10\x00\x00\x00\x0aUSE system\x00\x05"
        end

        it 'correctly encodes queries with multibyte characters' do
          bytes = RequestFrame.new(QueryRequest.new("INSERT INTO users (user_id, first, last, age) VALUES ('test', 'ümlaut', 'test', 1)", :all)).write(ByteBuffer.new)
          bytes.should eql_bytes("\x01\x00\x00\a\x00\x00\x00Y\x00\x00\x00SINSERT INTO users (user_id, first, last, age) VALUES ('test', '\xC3\xBCmlaut', 'test', 1)\x00\x05")
        end
      end

      context 'with PREPARE requests' do
        it 'encodes the request' do
          bytes = RequestFrame.new(PrepareRequest.new('UPDATE users SET email = ? WHERE user_name = ?')).write('')
          bytes.should == "\x01\x00\x00\x09\x00\x00\x00\x32\x00\x00\x00\x2eUPDATE users SET email = ? WHERE user_name = ?"
        end
      end

      context 'with EXECUTE requests' do
        let :id do
          "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
        end

        let :column_metadata do
          [['ks', 'tbl', 'col1', :varchar], ['ks', 'tbl', 'col2', :int], ['ks', 'tbl', 'col3', :varchar]]
        end

        it 'encodes the request' do
          bytes = RequestFrame.new(ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], :each_quorum)).write('')
          bytes.should == "\x01\x00\x00\x0a\x00\x00\x00\x2e\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/\x00\x03\x00\x00\x00\x05hello\x00\x00\x00\x04\x00\x00\x00\x2a\x00\x00\x00\x03foo\x00\x07"
        end

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
            buffer = RequestFrame.new(ExecuteRequest.new(id, metadata, [value], :one)).write(ByteBuffer.new)
            buffer.discard(8 + 2 + 16 + 2)
            length = buffer.unpack('N').first
            result_bytes = buffer[4, length]
            result_bytes.should eql_bytes(expected_bytes)
          end
        end

        it 'raises an error when the metadata and values don\'t have the same size' do
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 42], :each_quorum) }.to raise_error(ArgumentError)
        end

        it 'raises an error for unsupported column types' do
          column_metadata[2][3] = :imaginary
          expect { RequestFrame.new(ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], :each_quorum)).write('') }.to raise_error(UnsupportedColumnTypeError)
        end

        it 'raises an error for unsupported column collection types' do
          column_metadata[2][3] = [:imaginary, :varchar]
          expect { RequestFrame.new(ExecuteRequest.new(id, column_metadata, ['hello', 42, ['foo']], :each_quorum)).write('') }.to raise_error(UnsupportedColumnTypeError)
        end

        it 'raises an error when collection values are not enumerable' do
          column_metadata[2][3] = [:set, :varchar]
          expect { RequestFrame.new(ExecuteRequest.new(id, column_metadata, ['hello', 42, 'foo'], :each_quorum)).write('') }.to raise_error(InvalidValueError)
        end

        it 'raises an error when it cannot encode the argument' do
          expect { ExecuteRequest.new(id, column_metadata, ['hello', 'not an int', 'foo'], :each_quorum).write('') }.to raise_error(TypeError, /cannot be encoded as INT/)
        end
      end

      context 'with a stream ID' do
        it 'encodes the stream ID in the header' do
          bytes = RequestFrame.new(QueryRequest.new('USE system', :all), 42).write('')
          bytes[2].should == "\x2a"
        end

        it 'defaults to zero' do
          bytes = RequestFrame.new(QueryRequest.new('USE system', :all)).write('')
          bytes[2].should == "\x00"
        end

        it 'raises an exception if the stream ID is outside of 0..127' do
          expect { RequestFrame.new(QueryRequest.new('USE system', :all), -1) }.to raise_error(InvalidStreamIdError)
          expect { RequestFrame.new(QueryRequest.new('USE system', :all), 128) }.to raise_error(InvalidStreamIdError)
          expect { RequestFrame.new(QueryRequest.new('USE system', :all), 99999999) }.to raise_error(InvalidStreamIdError)
        end
      end

      describe StartupRequest do
        describe '#to_s' do
          it 'returns a pretty string' do
            request = StartupRequest.new
            request.to_s.should == 'STARTUP {"CQL_VERSION"=>"3.0.0"}'
          end
        end
      end

      describe CredentialsRequest do
        describe '#to_s' do
          it 'returns a pretty string' do
            request = CredentialsRequest.new('foo' => 'bar', 'hello' => 'world')
            request.to_s.should == 'CREDENTIALS {"foo"=>"bar", "hello"=>"world"}'
          end
        end

        describe '#eql?' do
          it 'returns when the credentials are the same' do
            c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
            c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
            c2.should eql(c2)
          end

          it 'returns when the credentials are equivalent' do
            pending 'this would be nice, but is hardly necessary' do
              c1 = CredentialsRequest.new(:username => 'foo', :password => 'bar')
              c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
              c1.should eql(c2)
            end
          end

          it 'returns false when the credentials are different' do
            c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'world')
            c2 = CredentialsRequest.new('username' => 'foo', 'hello' => 'world')
            c1.should_not eql(c2)
          end

          it 'is aliased as ==' do
            c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
            c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
            c1.should == c2
          end
        end

        describe '#hash' do
          it 'has the same hash code as another identical object' do
            c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
            c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
            c1.hash.should == c2.hash
          end

          it 'has the same hash code as another object with equivalent credentials' do
            pending 'this would be nice, but is hardly necessary' do
              c1 = CredentialsRequest.new(:username => 'foo', :password => 'bar')
              c2 = CredentialsRequest.new('username' => 'foo', 'password' => 'bar')
              c1.hash.should == c2.hash
            end
          end

          it 'does not have the same hash code when the credentials are different' do
            c1 = CredentialsRequest.new('username' => 'foo', 'password' => 'world')
            c2 = CredentialsRequest.new('username' => 'foo', 'hello' => 'world')
            c1.hash.should_not == c2.hash
          end
        end
      end

      describe OptionsRequest do
        describe '#to_s' do
          it 'returns a pretty string' do
            request = OptionsRequest.new
            request.to_s.should == 'OPTIONS'
          end
        end
      end

      describe RegisterRequest do
        describe '#to_s' do
          it 'returns a pretty string' do
            request = RegisterRequest.new('TOPOLOGY_CHANGE', 'STATUS_CHANGE')
            request.to_s.should == 'REGISTER ["TOPOLOGY_CHANGE", "STATUS_CHANGE"]'
          end
        end
      end

      describe QueryRequest do
        describe '#to_s' do
          it 'returns a pretty string' do
            request = QueryRequest.new('SELECT * FROM system.peers', :local_quorum)
            request.to_s.should == 'QUERY "SELECT * FROM system.peers" LOCAL_QUORUM'
          end
        end

        describe '#eql?' do
          it 'returns true when the CQL and consistency are the same' do
            q1 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2.should eql(q2)
          end

          it 'returns false when the consistency is different' do
            q1 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2 = QueryRequest.new('SELECT * FROM system.peers', :three)
            q1.should_not eql(q2)
          end

          it 'returns false when the CQL is different' do
            q1 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2 = QueryRequest.new('SELECT * FROM peers', :two)
            q1.should_not eql(q2)
          end

          it 'does not know about CQL syntax' do
            q1 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2 = QueryRequest.new('SELECT   *   FROM   system.peers', :two)
            q1.should_not eql(q2)
          end

          it 'is aliased as ==' do
            q1 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q1.should == q2
          end
        end

        describe '#hash' do
          it 'has the same hash code as another identical object' do
            q1 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q1.hash.should == q2.hash
          end

          it 'does not have the same hash code when the consistency is different' do
            q1 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2 = QueryRequest.new('SELECT * FROM system.peers', :three)
            q1.hash.should_not == q2.hash
          end

          it 'does not have the same hash code when the CQL is different' do
            q1 = QueryRequest.new('SELECT * FROM system.peers', :two)
            q2 = QueryRequest.new('SELECT * FROM peers', :two)
            q1.hash.should_not == q2.hash
          end
        end
      end

      describe PrepareRequest do
        describe '#to_s' do
          it 'returns a pretty string' do
            request = PrepareRequest.new('UPDATE users SET email = ? WHERE user_name = ?')
            request.to_s.should == 'PREPARE "UPDATE users SET email = ? WHERE user_name = ?"'
          end
        end

        describe '#eql?' do
          it 'returns true when the CQL is the same' do
            p1 = PrepareRequest.new('SELECT * FROM system.peers')
            p2 = PrepareRequest.new('SELECT * FROM system.peers')
            p1.should eql(p2)
          end

          it 'returns false when the CQL is different' do
            p1 = PrepareRequest.new('SELECT * FROM system.peers')
            p2 = PrepareRequest.new('SELECT * FROM peers')
            p1.should_not eql(p2)
          end

          it 'does not know about CQL syntax' do
            p1 = PrepareRequest.new('SELECT * FROM system.peers')
            p2 = PrepareRequest.new('SELECT   *   FROM   system.peers')
            p1.should_not eql(p2)
          end

          it 'is aliased as ==' do
            p1 = PrepareRequest.new('SELECT * FROM system.peers')
            p2 = PrepareRequest.new('SELECT * FROM system.peers')
            p1.should == p2
          end
        end

        describe '#hash' do
          it 'has the same hash code as another identical object' do
            p1 = PrepareRequest.new('SELECT * FROM system.peers')
            p2 = PrepareRequest.new('SELECT * FROM system.peers')
            p1.hash.should == p2.hash
          end

          it 'does not have the same hash code when the CQL is different' do
            p1 = PrepareRequest.new('SELECT * FROM system.peers')
            p2 = PrepareRequest.new('SELECT * FROM peers')
            p1.hash.should_not == p2.hash
          end
        end
      end

      describe ExecuteRequest do
        let :id do
          "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
        end

        let :metadata do
          [
            ['ks', 'tbl', 'col1', :varchar],
            ['ks', 'tbl', 'col2', :int],
            ['ks', 'tbl', 'col3', :varchar]
          ]
        end

        let :values do
          ['hello', 42, 'foo']
        end

        describe '#to_s' do
          it 'returns a pretty string' do
            request = ExecuteRequest.new(id, metadata, values, :each_quorum)
            request.to_s.should == 'EXECUTE ca487f1e7a82d23c4e8af3355171a52f ["hello", 42, "foo"] EACH_QUORUM'
          end
        end

        describe '#eql?' do
          it 'returns true when the ID, metadata, values and consistency are the same' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata, values, :one)
            e1.should eql(e2)
          end

          it 'returns false when the ID is different' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id.reverse, metadata, values, :one)
            e1.should_not eql(e2)
          end

          it 'returns false when the metadata is different' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata.reverse, values, :one)
            e1.should_not eql(e2)
          end

          it 'returns false when the values are different' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata, values.reverse, :one)
            e1.should_not eql(e2)
          end

          it 'returns false when the consistency is different' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata, values, :two)
            e1.should_not eql(e2)
          end

          it 'is aliased as ==' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata, values, :one)
            e1.should == e2
          end
        end

        describe '#hash' do
          it 'has the same hash code as another identical object' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata, values, :one)
            e1.hash.should == e2.hash
          end

          it 'does not have the same hash code when the ID is different' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id.reverse, metadata, values, :one)
            e1.hash.should_not == e2.hash
          end

          it 'does not have the same hash code when the metadata is different' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata.reverse, values, :one)
            e1.hash.should_not == e2.hash
          end

          it 'does not have the same hash code when the values are different' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata, values.reverse, :one)
            e1.hash.should_not == e2.hash
          end

          it 'does not have the same hash code when the consistency is different' do
            e1 = ExecuteRequest.new(id, metadata, values, :one)
            e2 = ExecuteRequest.new(id, metadata, values, :two)
            e1.hash.should_not == e2.hash
          end
        end
      end
    end
  end
end