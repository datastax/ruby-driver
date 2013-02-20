# encoding: utf-8

require 'spec_helper'


module Cql
  module Io
    describe IoReactor do
      let :host do
        Socket.gethostname
      end

      let :port do
        34535
      end

      let :server do
        FakeServer.new(port)
      end

      let :io_reactor do
        described_class.new(connection_timeout: 1)
      end

      before do
        server.start!
      end

      after do
        begin
          if io_reactor.running?
            io_reactor.stop.get
          end
        ensure
          server.stop!
        end
      end

      describe '#initialize' do
        it 'does not connect' do
          described_class.new
          sleep(0.1)
          server.connects.should == 0
        end
      end

      describe '#running?' do
        it 'is initially false' do
          io_reactor.should_not be_running
        end

        it 'is true when started' do
          io_reactor.start.get
          io_reactor.should be_running
        end

        it 'is true when starting' do
          f = io_reactor.start
          io_reactor.should be_running
          f.get
        end

        it 'is false when stopped' do
          io_reactor.start.get
          io_reactor.stop.get
          io_reactor.should_not be_running
        end
      end

      describe '#stop' do
        it 'closes all connections' do
          io_reactor.start.get
          f1 = io_reactor.add_connection(host, port)
          f2 = io_reactor.add_connection(host, port)
          f3 = io_reactor.add_connection(host, port)
          f4 = io_reactor.add_connection(host, port)
          Future.combine(f1, f2, f3, f4).get
          io_reactor.stop.get
          server.await_disconnects!(4)
          server.disconnects.should == 4
        end

        it 'succeeds connection futures when stopping while connecting' do
          f = io_reactor.add_connection(host, port + 9)
          io_reactor.start
          io_reactor.stop
          f.get
        end
      end

      describe '#add_connection' do
        before do
          io_reactor.start.get
        end

        it 'connects to the specified host and port' do
          future = io_reactor.add_connection(host, port)
          future.get
          server.await_connects!(1)
          server.connects.should == 1
        end

        it 'yields the connection ID when completed' do
          future = io_reactor.add_connection(host, port)
          future.get.should_not be_nil
        end

        it 'fails the returned future when it cannot connect to the host' do
          future = io_reactor.add_connection('example.com', port)
          expect { future.get }.to raise_error(ConnectionError)
        end

        it 'fails the returned future when it cannot connect to the port' do
          future = io_reactor.add_connection(host, 9999)
          expect { future.get }.to raise_error(ConnectionError)
        end

        it 'times out quickly when it cannot connect' do
          started_at = Time.now
          begin
            future = io_reactor.add_connection(host, 9999)
            future.get
          rescue ConnectionError
          end
          time_taken = (Time.now - started_at).to_f
          time_taken.should be < 1.5
        end

        it 'can be called before the reactor is started' do
          r = described_class.new(connection_timeout: 1)
          f1 = r.add_connection(host, port)
          f2 = r.start
          f2.get
          f1.get
        end
      end

      describe '#queue_request' do
        it 'eventually sends the request' do
          io_reactor.start
          io_reactor.add_connection(host, port).get
          io_reactor.queue_request(Cql::Protocol::StartupRequest.new)
          sleep(0.01) until server.received_bytes.size > 0
          server.received_bytes[3, 1].should == "\x01"
        end

        it 'can be called before the reactor is started' do
          io_reactor.queue_request(Cql::Protocol::StartupRequest.new)
          io_reactor.start
          io_reactor.add_connection(host, port).get
          sleep(0.01) until server.received_bytes.size > 0
          server.received_bytes[3, 1].should == "\x01"
        end

        it 'queues requests when all connections are busy' do
          request = Cql::Protocol::QueryRequest.new('UPDATE x SET y = 1 WHERE z = 2', :one)

          io_reactor.start
          io_reactor.add_connection(host, port).get

          futures = 200.times.map do
            io_reactor.queue_request(request)
          end

          128.times do |i|
            server.broadcast!("\x81\x00#{[i].pack('c')}\b\x00\x00\x00\x04\x00\x00\x00\x01")
          end

          Future.combine(*futures.shift(128)).get

          128.times do |i|
            server.broadcast!("\x81\x00#{[i].pack('c')}\b\x00\x00\x00\x04\x00\x00\x00\x01")
          end

          Future.combine(*futures).get
        end

        it 'performs the request using the connection with the given ID' do
          future = Future.new
          request = Cql::Protocol::StartupRequest.new
          response = "\x81\x00\x00\x02\x00\x00\x00\x00"

          io_reactor.start.on_complete do
            io_reactor.add_connection(host, port).on_complete do |c1_id|
              io_reactor.add_connection(host, port).on_complete do |c2_id|
                q1_future = io_reactor.queue_request(request, c2_id)
                q2_future = io_reactor.queue_request(request, c1_id)

                Future.combine(q1_future, q2_future).on_complete do |(_, q1_id), (_, q2_id)|
                  future.complete!([c1_id, c2_id, q1_id, q2_id])
                end

                server.await_connects!(2)
                server.broadcast!(response.dup)
              end
            end
          end

          connection1_id, connection2_id, query1_id, query2_id = future.value

          connection1_id.should_not be_nil
          connection2_id.should_not be_nil
          query1_id.should == connection2_id
          query2_id.should == connection1_id
        end

        it 'fails if the connection does not exist' do
          f = io_reactor.start.flat_map do
            io_reactor.add_connection(host, port).flat_map do
              io_reactor.queue_request(Cql::Protocol::StartupRequest.new, 1234)
            end
          end
          expect { f.get }.to raise_error(ConnectionNotFoundError)
        end

        it 'fails if the connection is busy' do
          f = io_reactor.start.flat_map do
            io_reactor.add_connection(host, port).flat_map do
              io_reactor.add_connection(host, port).flat_map do |connection_id|
                200.times do
                  io_reactor.queue_request(Cql::Protocol::OptionsRequest.new, connection_id)
                end
                io_reactor.queue_request(Cql::Protocol::OptionsRequest.new, connection_id)
              end
            end
          end
          expect { f.get }.to raise_error(ConnectionBusyError)
        end

        it 'fails if the connection is busy, when there is only one connection' do
          pending 'as it is the reactor doesn\'t try to deliver requests when all connections are busy'
        end

        it 'yields the response when completed' do
          response = nil
          io_reactor.start
          io_reactor.add_connection(host, port).get
          f = io_reactor.queue_request(Cql::Protocol::StartupRequest.new)
          f.on_complete do |r, _|
            response = r
          end
          server.broadcast!("\x81\x00\x00\x02\x00\x00\x00\x00")
          sleep(0.01) until response
          response.should == Cql::Protocol::ReadyResponse.new
        end

        it 'yields the connection ID when completed' do
          connection = nil
          io_reactor.start
          io_reactor.add_connection(host, port).get
          f = io_reactor.queue_request(Cql::Protocol::StartupRequest.new)
          f.on_complete do |_, c|
            connection = c
          end
          server.broadcast!("\x81\x00\x00\x02\x00\x00\x00\x00")
          sleep(0.01) until connection
          connection.should_not be_nil
        end
      end

      describe '#add_event_listener' do
        it 'calls the listener when frames with stream ID -1 arrives' do
          event = nil
          io_reactor.start
          io_reactor.add_connection(host, port).get
          io_reactor.add_event_listener { |e| event = e }
          server.broadcast!("\x81\x00\xFF\f\x00\x00\x00+\x00\rSCHEMA_CHANGE\x00\aDROPPED\x00\nkeyspace01\x00\x05users")
          sleep(0.01) until event
          event.should == Cql::Protocol::SchemaChangeEventResponse.new('DROPPED', 'keyspace01', 'users')
        end
      end

      context 'when errors occur' do
        context 'in the IO loop' do
          before do
            bad_request = stub(:request)
            bad_request.stub(:opcode).and_raise(StandardError.new('Blurgh'))
            io_reactor.start
            io_reactor.add_connection(host, port).get
            io_reactor.queue_request(bad_request)
          end

          it 'stops' do
            sleep(0.1)
            io_reactor.should_not be_running
          end

          it 'fails the future returned from #stop' do
            expect { io_reactor.stop.get }.to raise_error('Blurgh')
          end
        end
      end
    end
  end
end