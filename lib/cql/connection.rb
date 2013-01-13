# encoding: utf-8

require 'socket'


module Cql
  class Connection
    attr_reader :log

    def self.open(options={})
      options = {:host => 'localhost', :port => 9042}.merge(options)
      new(TCPSocket.new(options.delete(:host), options.delete(:port)), options)
    end

    def initialize(socket, options={})
      @socket = socket
      @trace_io = options[:trace_io]
    end

    def close
      @socket.close
    end

    def execute(request)
      frame = Cql::RequestFrame.new(request)
      frame.write(@socket)
      frame.write(@trace_io) if @trace_io
      @socket.flush
      receive
    end

    def receive
      frame = Cql::ResponseFrame.new
      until frame.complete?
        read_length = frame.body_length || frame.header_length
        bytes = @socket.read(read_length)
        @trace_io << bytes if @trace_io
        frame << bytes
      end
      frame.body
    end
  end
end