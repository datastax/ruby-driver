# encoding: utf-8

require 'socket'
require 'thread'


module Cql
  class Connection
    def initialize(options={})
      @host = options[:host] || 'localhost'
      @port = options[:port] || 9042
    end

    def open
      @request_queue = Queue.new
      @io_thread = Thread.start(@request_queue, &method(:io_loop))
      self
    end

    def close
      @closed = true
    end

    def closed?
      @closed
    end

    def execute(request, &handler)
      @request_queue << [request, handler]
      nil
    end

    def execute!(request)
      q = Queue.new
      execute(request) { |response| q << response }
      q.pop
    end

    private

    def io_loop(request_queue)
      stream = Stream.new(TCPSocket.new(@host, @port))
      until closed?
        request, handler = request_queue.pop
        request_frame = Cql::RequestFrame.new(request)
        stream.send_frame(request_frame)
        response_frame = stream.receive_frame
        handler.call(response_frame.body)
      end
    end

    class Stream
      def initialize(io)
        @io = io
      end

      def to_io
        @io
      end

      def send_frame(frame)
        frame.write(@io)
        @io.flush
      end

      def receive_frame
        frame = Cql::ResponseFrame.new
        until frame.complete?
          read_length = frame.body_length || frame.header_length
          bytes = @io.read(read_length)
          frame << bytes
        end
        frame
      end
    end
  end
end