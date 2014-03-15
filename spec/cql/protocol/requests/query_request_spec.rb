# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe QueryRequest do
      describe '#initialize' do
        it 'raises an error when the CQL is nil' do
          expect { QueryRequest.new(nil, [], [], :one, nil, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the consistency is nil' do
          expect { QueryRequest.new('USE system', [], [], nil, nil, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the consistency is invalid' do
          expect { QueryRequest.new('USE system', [], [], :hello, nil, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the serial consistency is invalid' do
          expect { QueryRequest.new('USE system', [], [], :quorum, :fantastic_serial, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when there are not the same number of type hints as bound values' do
          expect { QueryRequest.new('SELECT * FROM foo WHERE a = ? AND b = ?', ['x', 'y'], [:string], :quorum, nil, nil, nil, false) }.to raise_error(ArgumentError)
        end

        it 'raises an error when a paging state is given but no page size' do
          expect { QueryRequest.new('USE system', [], [], :quorum, nil, nil, 'foo', false) }.to raise_error(ArgumentError)
        end
      end

      describe '#write' do
        context 'when the protocol version is 1' do
          let :frame_bytes do
            QueryRequest.new('USE system', [], [], :all, nil, nil, nil, false).write(1, CqlByteBuffer.new)
          end

          it 'encodes the CQL' do
            frame_bytes.to_s[0, 14].should == "\x00\x00\x00\x0aUSE system"
          end

          it 'encodes the consistency' do
            frame_bytes.to_s[14, 999].should == "\x00\x05"
          end
        end

        context 'when the protocol version is 2' do
          context 'and there are no bound values' do
            let :frame_bytes do
              QueryRequest.new('USE system', [], [], :all, nil, nil, nil, false).write(2, CqlByteBuffer.new)
            end

            it 'encodes the CQL' do
              frame_bytes.to_s[0, 14].should == "\x00\x00\x00\x0aUSE system"
            end

            it 'encodes the consistency' do
              frame_bytes.to_s[14, 2].should == "\x00\x05"
            end

            it 'encodes an empty flag field' do
              frame_bytes.to_s[16, 999].should == "\x00"
            end

            it 'accepts that the bound values list is nil' do
              frame_bytes = QueryRequest.new('USE system', nil, [], :all, nil, nil, nil, false).write(2, CqlByteBuffer.new)
              frame_bytes.to_s[16, 999].should == "\x00"
            end

            it 'accepts that the type hints list is nil' do
              expect { QueryRequest.new('SELECT * FROM foo WHERE a = ? AND b = ?', ['x', 'y'], nil, :all, nil, nil, nil, false).write(2, CqlByteBuffer.new) }.to_not raise_error
            end
          end

          context 'and there are bound values' do
            let :cql do
              'SELECT * FROM something WHERE id = ?'
            end

            let :frame_bytes do
              QueryRequest.new(cql, ['foobar'], nil, :all, nil, nil, nil, false).write(2, CqlByteBuffer.new)
            end

            it 'encodes the CQL' do
              frame_bytes.to_s[0, 40].should == "\x00\x00\x00\x24SELECT * FROM something WHERE id = ?"
            end

            it 'encodes the consistency' do
              frame_bytes.to_s[40, 2].should == "\x00\x05"
            end

            it 'encodes a flags field with the values flag set' do
              frame_bytes.to_s[42, 1].should == "\x01"
            end

            it 'encodes the number of bound values' do
              frame_bytes.to_s[43, 2].should == "\x00\x01"
            end

            it 'encodes the bound values' do
              frame_bytes.to_s[45, 999].should == "\x00\x00\x00\x06foobar"
            end

            [
              ['foobar', 'VARCHAR', "\x00\x00\x00\x06foobar"],
              [765438000, 'BIGINT', "\x00\x00\x00\x08\x00\x00\x00\x00\x2d\x9f\xa8\x30"],
              [Math::PI, 'DOUBLE', "\x00\x00\x00\x08\x40\x09\x21\xfb\x54\x44\x2d\x18"],
              [67890656781923123918798273492834712837198237, 'VARINT', "\x00\x00\x00\x13\x03\x0b\x58\xb5\x74\x0a\xce\x65\x95\xb4\x03\x26\x7b\x6b\x6a\x6e\x08\x91\x9d"],
              [BigDecimal.new('1313123123.234234234234234234123'), 'DECIMAL', "\x00\x00\x00\x11\x00\x00\x00\x15\x10\x92\xed\xfd\x4b\x93\x4b\xd7\xa2\xc1\x0c\x65\x0b"],
              [true, 'BOOLEAN', "\x00\x00\x00\x01\x01"],
              [nil, 'null', "\xff\xff\xff\xff"],
              [Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'), 'UUID', "\x00\x00\x00\x10\x00\xb6\x91\x80\xd0\xe1\x11\xe2\x8b\x8b\x08\x00\x20\x0c\x9a\x66"],
              [IPAddr.new('200.199.198.197'), 'INET', "\x00\x00\x00\x04\xc8\xc7\xc6\xc5"],
              [IPAddr.new('2001:0db8:85a3:0000:0000:8a2e:0370:7334'), 'INET', "\x00\x00\x00\x10\x20\x01\x0d\xb8\x85\xa3\x00\x00\x00\x00\x8a\x2e\x03\x70\x73\x34"],
              [Time.utc(2013, 12, 11, 10, 9, 8), 'TIMESTAMP', "\x00\x00\x00\x08\x00\x00\x01\x42\xe1\x21\xa5\xa0"],
              [{'foo' => true}, 'MAP<STRING,BOOLEAN>', "\x00\x00\x00\x0a\x00\x01\x00\x03foo\x00\x01\x01"],
              [[1, 2], 'LIST<BIGINT>', "\x00\x00\x00\x16\x00\x02\x00\x08\x00\x00\x00\x00\x00\x00\x00\x01\x00\x08\x00\x00\x00\x00\x00\x00\x00\x02"],
              [[Math::PI, Math::PI/2].to_set, 'SET<DOUBLE>', "\x00\x00\x00\x16\x00\x02\x00\x08\x40\x09\x21\xfb\x54\x44\x2d\x18\x00\x08\x3f\xf9\x21\xfb\x54\x44\x2d\x18"],
            ].each do |value, cql_type, expected_bytes|
              it "encodes bound #{value.class}s as #{cql_type}" do
                frame_bytes = QueryRequest.new(cql, [value], nil, :all, nil, nil, nil, false).write(2, CqlByteBuffer.new)
                frame_bytes.to_s[45, 999].should == expected_bytes
              end
            end

            it 'complains if it cannot guess the type to encode a value as' do
              expect { QueryRequest.new(cql, [self], nil, :all, nil, nil, nil, false).write(2, CqlByteBuffer.new) }.to raise_error(EncodingError)
            end

            it 'uses the type hints to encode values' do
              frame_bytes = QueryRequest.new(cql, [4, 3.14], [:int, :float], :all, nil, nil, nil, false).write(2, CqlByteBuffer.new)
              frame_bytes.to_s[45, 8].should == "\x00\x00\x00\x04\x00\x00\x00\x04"
              frame_bytes.to_s[45 + 8, 8].should == "\x00\x00\x00\x04\x40\x48\xf5\xc3"
            end

            it 'accepts that some hints are nil and defaults to guessing' do
              frame_bytes = QueryRequest.new(cql, [4, 4], [:int, nil], :all, nil, nil, nil, false).write(2, CqlByteBuffer.new)
              frame_bytes.to_s[45, 8].should == "\x00\x00\x00\x04\x00\x00\x00\x04"
              frame_bytes.to_s[45 + 8, 12].should == "\x00\x00\x00\x08\x00\x00\x00\x00\x00\x00\x00\x04"
            end
          end

          context 'and the serial consistency is LOCAL_SERIAL' do
            it 'sets the serial flag' do
              frame_bytes = QueryRequest.new('UPDATE x SET y = 3 WHERE z = 4 IF w = 6', nil, nil, :two, :local_serial, nil, nil, false).write(2, CqlByteBuffer.new)
              frame_bytes.to_s[43, 2].should == "\x00\x02"
              frame_bytes.to_s[45, 1].should == "\x10"
              frame_bytes.to_s[46, 2].should == "\x00\x09"
            end
          end

          context 'and page size and/or page state is set' do
            it 'sets the page size flag and includes the page size' do
              frame_bytes = QueryRequest.new('SELECT * FROM users', nil, nil, :one, nil, 10, nil, false).write(2, CqlByteBuffer.new)
              frame_bytes.to_s[25, 1].should == "\x04"
              frame_bytes.to_s[26, 4].should == "\x00\x00\x00\x0a"
            end

            it 'sets both the page size and paging state flags and includes both the page size and the paging state' do
              frame_bytes = QueryRequest.new('SELECT * FROM users', nil, nil, :one, nil, 10, 'foo', false).write(2, CqlByteBuffer.new)
              frame_bytes.to_s[25, 1].should == "\x0c"
              frame_bytes.to_s[26, 4].should == "\x00\x00\x00\x0a"
              frame_bytes.to_s[30, 7].should == "\x00\x00\x00\x03foo"
            end
          end
        end

        context 'with multibyte characters' do
          it 'correctly encodes the frame' do
            bytes = QueryRequest.new("INSERT INTO users (user_id, first, last, age) VALUES ('test', 'ümlaut', 'test', 1)", nil, nil, :all, nil, nil, nil, false).write(1, CqlByteBuffer.new)
            bytes.should eql_bytes("\x00\x00\x00SINSERT INTO users (user_id, first, last, age) VALUES ('test', '\xC3\xBCmlaut', 'test', 1)\x00\x05")
          end
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :local_quorum, nil, nil, nil, false)
          request.to_s.should == 'QUERY "SELECT * FROM system.peers" LOCAL_QUORUM'
        end
      end

      describe '#eql?' do
        it 'returns true when the CQL and consistency are the same' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2.should eql(q2)
        end

        it 'returns false when the CQL is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, nil, nil, false)
          q1.should_not eql(q2)
        end

        it 'returns false when the values are different' do
          q1 = QueryRequest.new('SELECT * FROM peers WHERE data_center = ?', ['dc1'], nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers WHERE data_center = ?', ['dc2'], nil, :two, nil, nil, nil, false)
          q1.should_not eql(q2)
        end

        it 'returns false when the type hints are different' do
          q1 = QueryRequest.new('SELECT * FROM peers WHERE data_center = ?', ['dc1'], [:text], :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers WHERE data_center = ?', ['dc1'], [:varchar], :two, nil, nil, nil, false)
          q1.should_not eql(q2)
        end

        it 'returns false when the consistency is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :three, nil, nil, nil, false)
          q1.should_not eql(q2)
        end

        it 'returns false when the serial consistency is different' do
          q1 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, :local_serial, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, :serial, nil, nil, false)
          q1.should_not eql(q2)
        end

        it 'returns false when the page size is different' do
          q1 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, 10, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, 20, nil, false)
          q1.should_not eql(q2)
        end

        it 'returns false when the paging state is different' do
          q1 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, 10, 'foo', false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, 10, 'bar', false)
          q1.should_not eql(q2)
        end

        it 'does not know about CQL syntax' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT   *   FROM   system.peers', nil, nil, :two, nil, nil, nil, false)
          q1.should_not eql(q2)
        end

        it 'does not make a difference between an empty values array and nil' do
          q1 = QueryRequest.new('SELECT * FROM peers', [], nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, nil, nil, false)
          q1.should eql(q2)
        end

        it 'does not make a difference between an empty type hints array and nil' do
          q1 = QueryRequest.new('SELECT * FROM peers', nil, [], :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, nil, nil, false)
          q1.should eql(q2)
        end

        it 'is aliased as ==' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q1.should == q2
        end
      end

      describe '#hash' do
        it 'has the same hash code as another identical object' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q1.hash.should == q2.hash
        end

        it 'does not have the same hash code when the CQL is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, nil, nil, false)
          q1.hash.should_not == q2.hash
        end

        it 'does not have the same hash code when the values are different' do
          q1 = QueryRequest.new('SELECT * FROM peers WHERE data_center = ?', ['dc1'], nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers WHERE data_center = ?', ['dc2'], nil, :two, nil, nil, nil, false)
          q1.hash.should_not == q2.hash
        end

        it 'does not have the same hash code when the type hints are different' do
          q1 = QueryRequest.new('SELECT * FROM peers WHERE data_center = ?', ['dc1'], [:text], :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers WHERE data_center = ?', ['dc1'], [:varchar], :two, nil, nil, nil, false)
          q1.hash.should_not == q2.hash
        end

        it 'does not have the same hash code when the consistency is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :three, nil, nil, nil, false)
          q1.hash.should_not == q2.hash
        end

        it 'does not have the same hash code when the serial consistency is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, :local_serial, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, :serial, nil, nil, false)
          q1.hash.should_not == q2.hash
        end

        it 'does not have the same hash code when the page size is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, 10, nil, false)
          q2 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, 20, nil, false)
          q1.hash.should_not == q2.hash
        end

        it 'does not have the same hash code when the paging state is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, 10, 'foo', false)
          q2 = QueryRequest.new('SELECT * FROM system.peers', nil, nil, :two, nil, 10, 'bar', false)
          q1.hash.should_not == q2.hash
        end

        it 'does not make a difference between an empty values array and nil' do
          q1 = QueryRequest.new('SELECT * FROM peers', [], nil, :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, nil, nil, false)
          q1.hash.should == q2.hash
        end

        it 'does not make a difference between an empty type hints array and nil' do
          q1 = QueryRequest.new('SELECT * FROM peers', nil, [], :two, nil, nil, nil, false)
          q2 = QueryRequest.new('SELECT * FROM peers', nil, nil, :two, nil, nil, nil, false)
          q1.hash.should == q2.hash
        end
      end
    end
  end
end
