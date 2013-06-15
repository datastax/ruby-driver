# encoding: utf-8

require 'spec_helper'


module Cql
  module Io
    describe CqlConnection do
      let :connection do
        described_class.new(socket_handler)
      end

      let :socket_handler do
        stub(:socket_handler)
      end

      let :request do
        Protocol::OptionsRequest.new
      end

      let :buffer do
        ByteBuffer.new
      end

      describe '#initialize' do
        it 'registers as a data listener to the socket handler' do
          handler = nil
          socket_handler.stub(:on_data) do |&h|
            handler = h
          end
          c = described_class.new(socket_handler)
          handler.should_not be_nil
        end
      end

      describe '#send_request' do
        before do
          socket_handler.stub(:on_data) { |&h| socket_handler.stub(:data_callback).and_return(h) }
          socket_handler.stub(:write).and_yield(buffer)
        end

        it 'encodes a request frame and writes to the socket handler' do
          connection.send_request(request)
          buffer.to_s.should == [1, 0, 0, 5, 0].pack('C4N')
        end

        it 'encodes a frame with the next available stream ID' do
          connection.send_request(request)
          connection.send_request(request)
          buffer.to_s.should == [1, 0, 0, 5, 0].pack('C4N') + [1, 0, 1, 5, 0].pack('C4N')
        end

        it 'returns a future' do
          connection.send_request(request).should be_a(Future)
        end

        it 'completes the future when it receives a response frame with the corresponding stream ID' do
          3.times { connection.send_request(request) }
          future = connection.send_request(request)
          socket_handler.data_callback.call([0x81, 0, 3, 2, 0].pack('C4N'))
          await(0.1) { future.complete? }
        end

        it 'handles multiple response frames in the same data packet' do
          futures = Array.new(4) { connection.send_request(request) }
          socket_handler.data_callback.call([0x81, 0, 2, 2, 0].pack('C4N') + [0x81, 0, 3, 2, 0].pack('C4N'))
          await(0.1) { futures[2].complete? && futures[3].complete? }
        end

        it 'queues the request when there are too many in flight, sending it as soon as a stream is available' do
          socket_handler.stub(:write)
          futures = Array.new(130) { connection.send_request(request) }
          128.times { |i| socket_handler.data_callback.call([0x81, 0, i, 2, 0].pack('C4N')) }
          futures[127].should be_complete
          futures[128].should_not be_complete
          2.times { |i| socket_handler.data_callback.call([0x81, 0, i, 2, 0].pack('C4N')) }
          futures[128].should be_complete
        end
      end

      describe '#on_event' do
        before do
          socket_handler.stub(:on_data) { |&h| socket_handler.stub(:data_callback).and_return(h) }
        end

        it 'calls the callback on events' do
          event = nil
          connection.on_event do |e|
            event = e
          end
          socket_handler.data_callback.call("\x81\x00\xFF\f\x00\x00\x00+\x00\rSCHEMA_CHANGE\x00\aDROPPED\x00\x0cthe_keyspace\x00\x09the_table")
          event.should == Protocol::SchemaChangeEventResponse.new('DROPPED', 'the_keyspace', 'the_table')
        end
      end
    end
  end
end
