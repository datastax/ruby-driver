# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe QueryRequest do
      describe '#initialize' do
        it 'raises an error when the CQL is nil' do
          expect { QueryRequest.new(nil, [], :one) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the consistency is nil' do
          expect { QueryRequest.new('USE system', [], nil) }.to raise_error(ArgumentError)
        end

        it 'raises an error when the consistency is invalid' do
          expect { QueryRequest.new('USE system', [], :hello) }.to raise_error(ArgumentError)
        end
      end

      describe '#write' do
        context 'when the protocol version is 1' do
          let :frame_bytes do
            QueryRequest.new('USE system', [], :all).write(1, '')
          end

          it 'encodes the CQL' do
            frame_bytes.to_s[0, 14].should == "\x00\x00\x00\x0aUSE system"
          end

          it 'encodes the consistency' do
            frame_bytes.to_s[14, 999].should == "\x00\x05"
          end
        end

        context 'when the protocol version is 2' do
          context 'and there are no bound variables' do
            let :frame_bytes do
              QueryRequest.new('USE system', [], :all).write(2, '')
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
          end

          context 'and there are bound variables' do
            let :frame_bytes do
              QueryRequest.new('SELECT * FROM something WHERE id = ?', ['foobar'], :all).write(2, '')
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

            it 'encodes the bound variables' do
              frame_bytes.to_s[43, 999].should == "\x00\x01\x00\x00\x00\x06foobar"
            end
          end
        end

        context 'with multibyte characters' do
          it 'correctly encodes the frame' do
            bytes = QueryRequest.new("INSERT INTO users (user_id, first, last, age) VALUES ('test', 'ümlaut', 'test', 1)", [], :all).write(1, '')
            bytes.should eql_bytes("\x00\x00\x00SINSERT INTO users (user_id, first, last, age) VALUES ('test', '\xC3\xBCmlaut', 'test', 1)\x00\x05")
          end
        end
      end

      describe '#to_s' do
        it 'returns a pretty string' do
          request = QueryRequest.new('SELECT * FROM system.peers', [], :local_quorum)
          request.to_s.should == 'QUERY "SELECT * FROM system.peers" LOCAL_QUORUM'
        end
      end

      describe '#eql?' do
        it 'returns true when the CQL and consistency are the same' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2.should eql(q2)
        end

        it 'returns false when the consistency is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2 = QueryRequest.new('SELECT * FROM system.peers', [], :three)
          q1.should_not eql(q2)
        end

        it 'returns false when the CQL is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2 = QueryRequest.new('SELECT * FROM peers', [], :two)
          q1.should_not eql(q2)
        end

        it 'does not know about CQL syntax' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2 = QueryRequest.new('SELECT   *   FROM   system.peers', [], :two)
          q1.should_not eql(q2)
        end

        it 'is aliased as ==' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q1.should == q2
        end
      end

      describe '#hash' do
        it 'has the same hash code as another identical object' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q1.hash.should == q2.hash
        end

        it 'does not have the same hash code when the consistency is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2 = QueryRequest.new('SELECT * FROM system.peers', [], :three)
          q1.hash.should_not == q2.hash
        end

        it 'does not have the same hash code when the CQL is different' do
          q1 = QueryRequest.new('SELECT * FROM system.peers', [], :two)
          q2 = QueryRequest.new('SELECT * FROM peers', [], :two)
          q1.hash.should_not == q2.hash
        end
      end
    end
  end
end