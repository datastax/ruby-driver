# encoding: utf-8

require 'spec_helper'

module Cql
  describe(Builder) do
    let :builder do
      described_class.new(services)
    end

    let :io_reactor do
      FakeIoReactor.new
    end

    let :services do
      {:io_reactor => io_reactor}
    end

    def connections
      io_reactor.connections
    end

    def last_connection
      connections.last
    end

    def requests
      last_connection.requests
    end

    def last_request
      requests.last
    end

    def handle_request(&handler)
      @request_handler = handler
    end

    before do
      io_reactor.on_connection do |connection|
        connection.handle_request do |request, timeout|
          response = nil
          if @request_handler
            response = @request_handler.call(request, connection, proc { connection.default_request_handler(request) }, timeout)
          end
          unless response
            response = connection.default_request_handler(request)
          end
          response
        end
      end
    end

    describe "#build" do
      it 'connects to cluster localhost by default' do
        builder.build
        connections.map(&:host).should == %w[127.0.0.1]
      end
    end
  end
end
