# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe ResultResponse do
      describe '.decode' do
        it 'decodes a set_keyspace result' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x03\x00\x06system")
          response = described_class.decode(1, buffer, buffer.length)
          response.should_not be_void
          response.keyspace.should == 'system'
        end

        it 'decodes a schema_change CREATED result' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x05\x00\aCREATED\x00\ncql_rb_477\x00\x00")
          response = described_class.decode(1, buffer, buffer.length)
          response.should_not be_void
          response.change.should == 'CREATED'
          response.keyspace.should == 'cql_rb_477'
          response.table.should be_empty
        end

        it 'decodes a schema_change UPDATED result' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x05\x00\aUPDATED\x00\ncql_rb_973\x00\x05users")
          response = described_class.decode(1, buffer, buffer.length)
          response.should_not be_void
          response.change.should == 'UPDATED'
          response.keyspace.should == 'cql_rb_973'
          response.table.should == 'users'
        end

        it 'decodes a void result' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x01")
          response = described_class.decode(1, buffer, buffer.length)
          response.should be_void
        end

        it 'decodes a rows result' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00\x03\x00\ncql_rb_126\x00\x05users\x00\tuser_name\x00\r\x00\x05email\x00\r\x00\bpassword\x00\r\x00\x00\x00\x02\x00\x00\x00\x04phil\x00\x00\x00\rphil@heck.com\xFF\xFF\xFF\xFF\x00\x00\x00\x03sue\x00\x00\x00\rsue@inter.net\xFF\xFF\xFF\xFF")
          response = described_class.decode(1, buffer, buffer.length)
          response.should_not be_void
          response.rows.size.should == 2
          response.metadata.size.should == 3
        end

        it 'decodes a rows result' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x02\x00\x00\x00\x04\x00\x00\x00\x03\x00\x05email\x00\r\x00\bpassword\x00\r\x00\x00\x00\x02\x00\x00\x00\x04phil\x00\x00\x00\rphil@heck.com\xFF\xFF\xFF\xFF\x00\x00\x00\x03sue\x00\x00\x00\rsue@inter.net\xFF\xFF\xFF\xFF")
          response = described_class.decode(1, buffer, buffer.length)
          response.should_not be_void
        end

        it 'decodes a prepared result' do
          buffer = CqlByteBuffer.new("\x00\x00\x00\x04\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/\x00\x00\x00\x01\x00\x00\x00\x01\x00\ncql_rb_911\x00\x05users\x00\tuser_name\x00\r")
          response = described_class.decode(1, buffer, buffer.length)
          response.should_not be_void
          response.id.should == "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
          response.metadata.size.should == 1
        end

        it 'complains when asked to decode an unknown result type' do
          expect { described_class.decode(1, CqlByteBuffer.new("\x00\x00\x00\xffhello"), 9) }.to raise_error(UnsupportedResultKindError)
        end
      end
    end
  end
end
