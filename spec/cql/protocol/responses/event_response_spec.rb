# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe EventResponse do
      describe '.decode' do
        it 'decodes a SCHEMA_CHANGE event' do
          buffer = CqlByteBuffer.new("\x00\rSCHEMA_CHANGE\x00\aDROPPED\x00\ncql_rb_609\x00\x05users")
          response = described_class.decode(1, buffer, buffer.length)
          response.type.should == 'SCHEMA_CHANGE'
          response.change.should == 'DROPPED'
          response.keyspace.should == 'cql_rb_609'
          response.table.should == 'users'
        end

        it 'decodes a STATUS_CHANGE event' do
          buffer = CqlByteBuffer.new("\x00\rSTATUS_CHANGE\x00\x04DOWN\x04\x00\x00\x00\x00\x00\x00#R")
          response = described_class.decode(1, buffer, buffer.length)
          response.type.should == 'STATUS_CHANGE'
          response.change.should == 'DOWN'
          response.address.should == IPAddr.new('0.0.0.0')
          response.port.should == 9042
        end

        it 'decodes a TOPOLOGY_CHANGE event' do
          buffer = CqlByteBuffer.new("\x00\x0FTOPOLOGY_CHANGE\x00\fREMOVED_NODE\x04\x00\x00\x00\x00\x00\x00#R")
          response = described_class.decode(1, buffer, buffer.length)
          response.type.should == 'TOPOLOGY_CHANGE'
          response.change.should == 'REMOVED_NODE'
          response.address.should == IPAddr.new('0.0.0.0')
          response.port.should == 9042
        end

        it 'complains when asked to decode an unknown event type' do
          expect { described_class.decode(1, CqlByteBuffer.new("\x00\x04PING"), 6) }.to raise_error(UnsupportedEventTypeError, /PING/)
        end
      end
    end
  end
end
