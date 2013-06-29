# encoding: utf-8

require 'spec_helper'


module Cql
  module Protocol
    describe CqlProtocolHandler do
      let :protocol_handler do
        described_class.new(connection)
      end

      let :connection do
        stub(:connection)
      end

      let :request do
        Protocol::OptionsRequest.new
      end

      let :buffer do
        ByteBuffer.new
      end

      before do
        connection.stub(:on_data) do |&h|
          connection.stub(:data_listener).and_return(h)
        end
        connection.stub(:on_closed) do |&h|
          connection.stub(:closed_listener).and_return(h)
        end
        connection.stub(:on_connected) do |&h|
          connection.stub(:connected_listener).and_return(h)
        end
        protocol_handler
      end

      describe '#initialize' do
        it 'registers as a data listener to the socket handler' do
          connection.data_listener.should_not be_nil
        end
      end

      describe '#send_request' do
        before do
          connection.stub(:write).and_yield(buffer)
          connection.stub(:closed?).and_return(false)

          connection.stub(:connected?).and_return(true)
        end

        it 'encodes a request frame and writes to the socket handler' do
          protocol_handler.send_request(request)
          buffer.to_s.should == [1, 0, 0, 5, 0].pack('C4N')
        end

        it 'encodes a frame with the next available stream ID' do
          protocol_handler.send_request(request)
          protocol_handler.send_request(request)
          buffer.to_s.should == [1, 0, 0, 5, 0].pack('C4N') + [1, 0, 1, 5, 0].pack('C4N')
        end

        it 'returns a future' do
          protocol_handler.send_request(request).should be_a(Future)
        end

        it 'completes the future when it receives a response frame with the corresponding stream ID' do
          3.times { protocol_handler.send_request(request) }
          future = protocol_handler.send_request(request)
          connection.data_listener.call([0x81, 0, 3, 2, 0].pack('C4N'))
          await(0.1) { future.complete? }
        end

        it 'handles multiple response frames in the same data packet' do
          futures = Array.new(4) { protocol_handler.send_request(request) }
          connection.data_listener.call([0x81, 0, 2, 2, 0].pack('C4N') + [0x81, 0, 3, 2, 0].pack('C4N'))
          await(0.1) { futures[2].complete? && futures[3].complete? }
        end

        it 'queues the request when there are too many in flight, sending it as soon as a stream is available' do
          connection.stub(:write)
          futures = Array.new(130) { protocol_handler.send_request(request) }
          128.times { |i| connection.data_listener.call([0x81, 0, i, 2, 0].pack('C4N')) }
          futures[127].should be_complete
          futures[128].should_not be_complete
          2.times { |i| connection.data_listener.call([0x81, 0, i, 2, 0].pack('C4N')) }
          futures[128].should be_complete
        end

        context 'when the protocol handler closes' do
          it 'fails all requests waiting for a reply' do
            futures = Array.new(5) { protocol_handler.send_request(request) }
            connection.closed_listener.call(StandardError.new('Blurgh'))
            futures.should be_all(&:failed?), 'expected all requests to have failed'
          end

          it 'fails all queued requests' do
            futures = Array.new(200) { protocol_handler.send_request(request) }
            connection.closed_listener.call(StandardError.new('Blurgh'))
            futures.should be_all(&:failed?), 'expected all requests to have failed'
          end

          it 'passes the error that caused the protocol handler to close to the failed requests' do
            error = nil
            future = protocol_handler.send_request(request)
            future.on_failure { |e| error = e }
            connection.closed_listener.call(StandardError.new('Blurgh'))
            error.should == StandardError.new('Blurgh')
          end
        end

        context 'when the protocol handler has closed' do
          it 'fails all requests with NotConnectedError' do
            connection.stub(:closed?).and_return(true)
            f = protocol_handler.send_request(request)
            expect { f.get }.to raise_error(NotConnectedError)
          end
        end
      end

      describe '#close' do
        it 'closes the underlying protocol handler' do
          connection.should_receive(:close)
          protocol_handler.close
        end

        context 'returns a future which' do
          it 'completes when the socket has closed' do
            connection.stub(:close) do
              connection.closed_listener.call(nil)
            end
            protocol_handler.close.get
          end
        end
      end

      describe '#keyspace' do
        before do
          connection.stub(:closed?).and_return(false)
          connection.stub(:connected?).and_return(true)
          connection.stub(:write)
        end

        it 'is not in a keyspace initially' do
          protocol_handler.keyspace.should be_nil
        end

        it 'registers the keyspace it has changed to' do
          f = protocol_handler.send_request(Protocol::QueryRequest.new('USE hello', :one))
          connection.data_listener.call([0x81, 0, 0, 8, 4 + 2 + 5, 3, 5].pack('C4N2n') + 'hello')
          f.get
          protocol_handler.keyspace.should == 'hello'
        end
      end

      [:connected?, :closed?].each do |message|
        describe "##{message}" do
          it 'reflects the underlying protocol handler\'s status' do
            connection.stub(message).and_return(true)
            protocol_handler.send(message).should be_true
            connection.stub(message).and_return(false)
            protocol_handler.send(message).should be_false
          end
        end
      end

      describe '#on_event' do
        it 'calls the callback on events' do
          event = nil
          protocol_handler.on_event do |e|
            event = e
          end
          connection.data_listener.call("\x81\x00\xFF\f\x00\x00\x00+\x00\rSCHEMA_CHANGE\x00\aDROPPED\x00\x0cthe_keyspace\x00\x09the_table")
          event.should == Protocol::SchemaChangeEventResponse.new('DROPPED', 'the_keyspace', 'the_table')
        end

        it 'ignores errors raised by the listener' do
          called = false
          protocol_handler.on_event { |e| raise 'Blurgh' }
          protocol_handler.on_event { |e| called = true }
          connection.data_listener.call("\x81\x00\xFF\f\x00\x00\x00+\x00\rSCHEMA_CHANGE\x00\aDROPPED\x00\x0cthe_keyspace\x00\x09the_table")
          called.should be_true, 'expected all event listeners to have been called'
        end
      end

      describe '#on_closed' do
        it 'calls the callback when the underlying protocol handler closes' do
          called = false
          protocol_handler.on_closed { called = true }
          connection.closed_listener.call(StandardError.new('Blurgh'))
          called.should be_true, 'expected the close listener to have been called'
        end

        it 'ignores errors raised by the listener' do
          called = false
          protocol_handler.on_closed { |e| raise 'Blurgh' }
          protocol_handler.on_closed { |e| called = true }
          connection.closed_listener.call(StandardError.new('Blurgh'))
          called.should be_true, 'expected all event listeners to have been called'
        end
      end
    end
  end
end
