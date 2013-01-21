# encoding: utf-8

require 'spec_helper'


module Cql
  describe Connection do
    describe '#initialize' do
      it 'does not connect' do
        described_class.new
      end
    end

    describe '#open' do
      let :host do
        Socket.gethostname
      end

      let :port do
        3453
      end

      let :connection do
        described_class.new(host: host, port: port)
      end

      def start_server!
        @server_running = [true]
        @connects = []
        @server = TCPServer.new(port)
        @server_thread = Thread.start(@server, @server_running, @connects) do |server, server_running, connects|
          Thread.current.abort_on_exception = true
          while server_running[0]
            begin
              connection = server.accept_nonblock
              connects << 1
              connection.close
            rescue Errno::EAGAIN
            end
          end
        end
      end

      def stop_server!
        return unless @server_running[0]
        @server_running[0] = false
        @server_thread.join
        @server.close
      end

      before do
        start_server!
      end

      after do
        connection.close unless connection.closed?
        stop_server!
      end

      it 'connects to the specified host and port' do
        connection.open
        sleep 0.1
        stop_server!
        @connects.should have(1).items
      end

      it 'does nothing when called a second time' do
        connection.open
        sleep 0.1
        connection.open
        sleep 0.1
        stop_server!
        @connects.should have(1).items
      end

      it 'returns the connection' do
        connection.open.should equal(connection)
      end

      it 'raises an error if it cannot connect' do
        expect { described_class.new(host: 'huffabuff.local', timeout: 1).open }.to raise_error(ConnectionError)
        expect { described_class.new(port: 9999, timeout: 1).open }.to raise_error(ConnectionError)
      end
    end
  end
end