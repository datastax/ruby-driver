# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe PrepareRequest do
      describe '#initialize' do
        it 'raises an error when the CQL is nil' do
          expect { PrepareRequest.new(nil) }.to raise_error(ArgumentError)
        end
      end

      describe '#write' do
        it 'encodes a PREPARE request frame' do
          bytes = PrepareRequest.new('UPDATE users SET email = ? WHERE user_name = ?').write(1, CqlByteBuffer.new)
          bytes.should eql_bytes("\x00\x00\x00\x2eUPDATE users SET email = ? WHERE user_name = ?")
        end
      end

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
  end
end