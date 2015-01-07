# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require 'spec_helper'


module Cassandra
  module Protocol
    describe CqlProtocolHandler do
      let :protocol_handler do
        described_class.new(connection, scheduler, 1)
      end

      let :connection do
        double(:connection)
      end

      let :scheduler do
        FakeIoReactor.new
      end

      let :request do
        Protocol::OptionsRequest.new
      end

      let :buffer do
        CqlByteBuffer.new
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

        it 'terminates connection after idle timeout' do
          connection.stub(:write).and_yield(buffer)
          connection.stub(:closed?).and_return(false)
          connection.stub(:connected?).and_return(true)

          protocol_handler.send_request(request)
          connection.data_listener.call([0x81, 0, 0, 2, 0].pack('C4N'))
          buffer.discard(buffer.length)

          expect(connection).to receive(:close)
          scheduler.advance_time(60)
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

        it 'schedules a heartbeat' do
          protocol_handler.send_request(request)
          expect(buffer).to_not be_empty
          buffer.discard(buffer.length)
          expect(buffer).to be_empty
          scheduler.advance_time(30)
          expect(buffer.to_s).to eq([1, 0, 1, 5, 0].pack('C4N'))
        end

        it 'schedules only one heartbeat' do
          10.times { protocol_handler.send_request(request) }
          buffer.discard(buffer.length)
          scheduler.advance_time(30)
          expect(buffer.to_s).to eq([1, 0, 10, 5, 0].pack('C4N'))
        end

        it 'schedules next heartbeat after response' do
          protocol_handler.send_request(request)
          connection.data_listener.call([0x81, 0, 0, 2, 0].pack('C4N'))
          buffer.discard(buffer.length)

          scheduler.advance_time(30)
          buffer.discard(buffer.length)
          connection.data_listener.call([0x81, 0, 0, 2, 0].pack('C4N'))

          scheduler.advance_time(30)
          expect(buffer.to_s).to eq([1, 0, 0, 5, 0].pack('C4N'))
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
          protocol_handler.send_request(request).should be_a(Ione::Future)
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

        context 'when a compressor is specified' do
          let :protocol_handler do
            described_class.new(connection, scheduler, 1, compressor)
          end

          let :compressor do
            double(:compressor)
          end

          let :request do
            Protocol::PrepareRequest.new('SELECT * FROM things')
          end

          before do
            compressor.stub(:compress?).and_return(true)
            compressor.stub(:compress).and_return('FAKECOMPRESSEDBODY')
          end

          it 'compresses request frames' do
            protocol_handler.send_request(request)
            buffer.to_s.should == [1, 1, 0, 9, 18].pack('C4N') + 'FAKECOMPRESSEDBODY'
          end

          it 'doesn\'t compress queued request frames' do
            130.times { protocol_handler.send_request(request) }
            expect(compressor).to have_received(:compress).exactly(128).times
          end

          it 'decompresses response frames' do
            id = "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/".force_encoding(::Encoding::BINARY)
            compressor.stub(:decompress).with('FAKECOMPRESSEDBODY').and_return("\x00\x00\x00\x04" + "\x00\x10" + id + "\x00\x00\x00\x01\x00\x00\x00\x01\x00\ncql_rb_911\x00\x05users\x00\tuser_name\x00\r")
            f1 = protocol_handler.send_request(request)
            f2 = protocol_handler.send_request(request)
            connection.data_listener.call("\x81\x01\x00\x08\x00\x00\x00\x12FAKECOMPRESSEDBODY")
            connection.data_listener.call("\x81\x01\x01\x08\x00\x00\x00\x12FAKECOMPRESSEDBODY")
            f1.value.should == Protocol::PreparedResultResponse.new(id, [["cql_rb_911", "users", "user_name", :varchar]], nil, nil)
            f2.value.should == Protocol::PreparedResultResponse.new(id, [["cql_rb_911", "users", "user_name", :varchar]], nil, nil)
          end
        end

        context 'when a protocol version is specified' do
          let :protocol_handler do
            described_class.new(connection, scheduler, 7)
          end

          it 'sets the protocol version in the header' do
            protocol_handler.send_request(request)
            buffer.to_s[0].should == "\x07"
          end
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

          it 'fails all requests with Errors::IOError if there is no specific error' do
            protocol_handler.send_request(request)
            future = protocol_handler.send_request(request)
            connection.data_listener.call([0x81, 0, 0, 2, 0].pack('C4N'))
            connection.closed_listener.call(nil)
            begin
              future.value
            rescue => e
              e.should be_a(Errors::IOError)
            else
              fail('No error was raised!')
            end
          end

          it 'changes the error that caused the protocol handler to close to the failed requests' do
            error = nil
            future = protocol_handler.send_request(request)
            future.on_failure { |e| error = e }
            connection.closed_listener.call(StandardError.new('Blurgh'))
            error.should == Cassandra::Errors::IOError.new('Blurgh')
          end
        end

        context 'when the protocol handler has closed' do
          it 'fails all requests with Errors::IOError' do
            connection.stub(:closed?).and_return(true)
            f = protocol_handler.send_request(request)
            expect { f.value }.to raise_error(Errors::IOError)
          end
        end

        context 'when the request times out' do
          it 'raises a TimeoutError' do
            f = protocol_handler.send_request(request, 3)
            scheduler.advance_time(3)
            expect { f.value }.to raise_error(Errors::TimeoutError)
          end

          it 'does not attempt to fulfill the promise when the request has already timed out' do
            f = protocol_handler.send_request(request, 3)
            scheduler.advance_time(3)
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
              protocol_handler.send_request(request)
            end
            f = protocol_handler.send_request(request, 5)
            scheduler.advance_time(5)
            128.times { |i| connection.data_listener.call([0x81, 0, i, 2, 0].pack('C4N')) }
            write_count.should == 128
          end
        end
      end

      describe '#close' do
        it 'closes the underlying protocol handler' do
          connection.should_receive(:close)
          protocol_handler.close
          scheduler.advance_time(0)
        end

        it 'returns a future which succeeds when the socket has closed' do
          connection.stub(:close) do
            connection.closed_listener.call(nil)
          end
          f = protocol_handler.close
          scheduler.advance_time(0)
          f.value
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
          f = protocol_handler.send_request(Protocol::QueryRequest.new('USE hello', EMPTY_LIST, EMPTY_LIST, :one))
          connection.data_listener.call([0x81, 0, 0, 8, 4 + 2 + 5, 3, 5].pack('C4N2n') + 'hello')
          f.value
          protocol_handler.keyspace.should == 'hello'
        end
      end

      [:connected?, :closed?].each do |message|
        describe "##{message}" do
          it 'reflects the underlying protocol handler\'s status' do
            connection.stub(message).and_return(true)
            protocol_handler.send(message).should be_truthy
            connection.stub(message).and_return(false)
            protocol_handler.send(message).should be_falsey
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
      end

      describe '#on_closed' do
        it 'calls the callback when the underlying protocol handler closes' do
          called = false
          protocol_handler.on_closed { called = true }
          connection.closed_listener.call(StandardError.new('Blurgh'))
          called.should be_truthy, 'expected the close listener to have been called'
        end

        it 'passes the error that made the connection close to the listener' do
          error = nil
          protocol_handler.on_closed { |e| error = e }
          connection.closed_listener.call(StandardError.new('Blurgh'))
          error.message.should == 'Blurgh'
        end

        it 'ignores errors raised by the listener' do
          called = false
          protocol_handler.on_closed { |e| raise 'Blurgh' }
          protocol_handler.on_closed { |e| called = true }
          connection.closed_listener.call(StandardError.new('Blurgh'))
          called.should be_truthy, 'expected all event listeners to have been called'
        end
      end
    end
  end
end
