# encoding: utf-8

require 'spec_helper'


module Cql
  module Io
    describe IoReactor do
      include AsyncHelpers
      include FakeServerHelpers

      let :host do
        Socket.gethostname
      end

      let :port do
        34535
      end

      let :io_reactor do
        described_class.new(connection_timeout: 1)
      end

      def await_server
        sleep 0.1
      end

      before do
        start_server!(port)
      end

      after do
        io_reactor.stop.get if io_reactor.running?
        stop_server!
      end

      describe '#initialize' do
        it 'does not connect' do
          described_class.new
          await_server
          server_stats[:connects].should == 0
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
          await_server
          server_stats[:disconnects].should == 4
        end
      end

      describe '#add_connection' do
        before do
          io_reactor.start.get
        end

        it 'connects to the specified host and port' do
          future = io_reactor.add_connection(host, port)
          future.get
          await_server
          server_stats[:connects].should == 1
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
          await do |signal|
            r.add_connection(host, port).on_complete do
              signal << :ping
            end
            r.start.get
          end
          r.stop.get
        end
      end

      describe '#queue_request' do
        it 'eventually sends the request' do
          r = described_class.new(connection_timeout: 1)
          r.start
          r.add_connection(host, port).get
          r.queue_request(Cql::Protocol::StartupRequest.new)
          sleep(0.1)
          r.stop.get
          sleep(0.1)
          server_stats[:data][3, 1].should == "\x01"
        end

        it 'can be called before the reactor is started' do
          r = described_class.new(connection_timeout: 1)
          r.queue_request(Cql::Protocol::StartupRequest.new)
          r.start
          r.add_connection(host, port).get
          sleep(0.1)
          r.stop.get
          sleep(0.1)
          server_stats[:data][3, 1].should == "\x01"
        end
      end

      describe '#add_event_listener' do
        it 'calls the listener when frames with stream ID -1 arrives'
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