# encoding: utf-8

require 'spec_helper'


module Cql
  module Io
    describe Connection do
      include AsyncHelpers
      include FakeServerHelpers

      let :host do
        Socket.gethostname
      end

      let :port do
        34535
      end

      let :connection do
        described_class.new(host: host, port: port)
      end

      def await_server
        sleep 0.1
      end

      before do
        start_server!(port)
      end

      after do
        connection.close.get if connection.connected?
        stop_server!
      end

      describe '#initialize' do
        it 'does not connect' do
          described_class.new
        end
      end

      describe '#connect' do
        it 'connects to the specified host and port' do
          future = connection.connect
          future.get
          await_server
          server_stats[:connects].should == 1
        end

        it 'does nothing when called a second time' do
          future = connection.connect
          future.get
          future = connection.connect
          future.get
          await_server
          server_stats[:connects].should == 1
        end

        it 'connects only once when called asynchronously' do
          future1 = connection.connect
          future2 = connection.connect
          future1.get
          future2.get
          await_server
          server_stats[:connects].should == 1
        end

        it 'fails the returned future when it cannot connect' do
          future = described_class.new(host: 'huffabuff.local', timeout: 1).connect
          expect { future.get }.to raise_error(ConnectionError)
          future = described_class.new(port: 9999, timeout: 1).connect
          expect { future.get }.to raise_error(ConnectionError)
        end

        it 'times out quickly when it cannot connect' do
          started_at = Time.now
          begin
            connection = described_class.new(port: 9999, timeout: 1)
            future = connection.connect
            future.get
          rescue ConnectionError
          end
          time_taken = (Time.now - started_at).to_f
          time_taken.should be < 1.5
        end
      end

      describe '#connected?' do
        it 'is initially false' do
          connection.should_not be_connected
        end

        it 'is true when connected' do
          value = :bad_value
          await do |signal|
            connection.connect.on_complete do
              value = connection.connected?
              signal << :ping
            end
          end
          value.should be_true
        end

        it 'is false when the connection has been closed' do
          value = :bad_value
          await do |signal|
            connection.connect.on_complete do
              connection.close.on_complete do
                value = connection.connected?
                signal << :ping
              end
            end
          end
          value.should be_false
        end
      end

      describe '#close' do
        it 'raises an error unless the connection is open' do
          expect { connection.close }.to raise_error(IllegalStateError)
        end

        it 'closes the connection' do
          await do |signal|
            connection.connect.on_complete do
              connection.close.on_complete do
                signal << :ping
              end
            end
          end
          await_server
          server_stats[:disconnects].should == 1
        end
      end

      describe '#closed?' do
        it 'is false initially' do
          connection.should_not be_closed
        end

        it 'is false while connected' do
          value = :bad_value
          await do |signal|
            connection.connect.on_complete do
              value = connection.closed?
              signal << :ping
            end
          end
          value.should be_false
        end

        it 'is true when the connection is closed' do
          await do |signal|
            connection.connect.on_complete do
              connection.close.on_complete do
                signal << :ping
              end
            end
          end
          connection.should be_closed
        end

        it 'is true when the connection is scheduled to close' do
          value = :bad_value
          await do |signal|
            connection.connect.on_complete do
              connection.close
              value = connection.closed?
              signal << :ping
            end
          end
          value.should be_true
        end
      end
    end
  end
end