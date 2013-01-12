# encoding: utf-8

require 'socket'


module Cql
  class Connection
    attr_reader :log

    def self.open(host, port)
      new(TCPSocket.new(host, port))
    end

    def initialize(socket)
      @socket = socket
    end

    def close
      @socket.close
    end

    def execute(request)
      frame = Cql::RequestFrame.new(request)
      frame.write(@socket)
      @socket.flush
      receive
    end

    def receive
      frame = Cql::ResponseFrame.new
      until frame.complete?
        frame << @socket.read(frame.length ? frame.length : 8)
      end
      frame.body
    end
  end
end