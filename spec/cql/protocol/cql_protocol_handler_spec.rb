# encoding: utf-8

require 'spec_helper'


module Cql
  module Protocol
    describe CqlProtocolHandler do
      let :protocol_handler do
        described_class.new(connection, scheduler)
      end

      let :connection do
        double(:connection)
      end

      let :scheduler do
        double(:scheduler)
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
        scheduler.stub(:schedule_timer).and_return(Promise.new.future)
        protocol_handler
      end

      describe '#initialize' do
        it 'registers as a data listener to the socket handler' do
          connection.data_listener.should_not be_nil
        end
      end

      describe '#[]/#[]=' do
        it 'is thread safe storage for arbitrary data' do
          protocol_handler[:host_id] = 42
          protocol_handler[:host_id].should == 42
        end
      end

      describe '#host' do
        it 'delegates to the connection' do
          connection.stub(:host).and_return('example.com')
          protocol_handler.host.should == 'example.com'
        end
      end

      describe '#port' do
        it 'delegates to the connection' do
          connection.stub(:port).and_return(9042)
          protocol_handler.port.should == 9042
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

        it 'succeeds the future when it receives a response frame with the corresponding stream ID' do
          3.times { protocol_handler.send_request(request) }
          future = protocol_handler.send_request(request)
          connection.data_listener.call([0x81, 0, 3, 2, 0].pack('C4N'))
          await(0.1) { future.resolved? }
        end

        it 'handles multiple response frames in the same data packet' do
          futures = Array.new(4) { protocol_handler.send_request(request) }
          connection.data_listener.call([0x81, 0, 2, 2, 0].pack('C4N') + [0x81, 0, 3, 2, 0].pack('C4N'))
          await(0.1) { futures[2].resolved? && futures[3].resolved? }
        end

        it 'queues the request when there are too many in flight, sending it as soon as a stream is available' do
          connection.stub(:write)
          futures = Array.new(130) { protocol_handler.send_request(request) }
          128.times { |i| connection.data_listener.call([0x81, 0, i, 2, 0].pack('C4N')) }
          futures[127].should be_resolved
          futures[128].should_not be_resolved
          2.times { |i| connection.data_listener.call([0x81, 0, i, 2, 0].pack('C4N')) }
          futures[128].should be_resolved
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

          it 'fails all requests with ConnectionClosedError if there is no specific error' do
            protocol_handler.send_request(request)
            future = protocol_handler.send_request(request)
            connection.data_listener.call([0x81, 0, 0, 2, 0].pack('C4N'))
            connection.closed_listener.call(nil)
            begin
              future.value
            rescue => e
              e.should be_a(Cql::Io::ConnectionClosedError)
            else
              fail('No error was raised!')
            end
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
            expect { f.value }.to raise_error(NotConnectedError)
          end
        end

        context 'when the request times out' do
          let :timer_promise do
            Promise.new
          end

          before do
            scheduler.stub(:schedule_timer).with(5).and_return(timer_promise.future)
          end

          it 'raises a TimeoutError' do
            f = protocol_handler.send_request(request)
            timer_promise.fulfill
            expect { f.value }.to raise_error(TimeoutError)
          end

          it 'does not attempt to fulfill the promise when the request has already timed out' do
            f = protocol_handler.send_request(request)
            timer_promise.fulfill
            expect { connection.data_listener.call([0x81, 0, 0, 2, 0].pack('C4N')) }.to_not raise_error
          end

          it 'never sends a request when it has already timed out' do
            write_count = 0
            connection.stub(:write) do |s, &h|
              write_count += 1
              if h
                h.call(buffer)
              else
                buffer << s
              end
            end
            128.times do
              scheduler.stub(:schedule_timer).with(5).and_return(Promise.new.future)
              protocol_handler.send_request(request)
            end
            scheduler.stub(:schedule_timer).with(5).and_return(timer_promise.future)
            f = protocol_handler.send_request(request)
            timer_promise.fulfill
            128.times { |i| connection.data_listener.call([0x81, 0, i, 2, 0].pack('C4N')) }
            write_count.should == 128
          end
        end
      end

      describe '#close' do
        it 'closes the underlying protocol handler' do
          connection.should_receive(:close)
          protocol_handler.close
        end

        it 'returns a future which succeeds when the socket has closed' do
          connection.stub(:close) do
            connection.closed_listener.call(nil)
          end
          protocol_handler.close.value
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
          f.value
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
