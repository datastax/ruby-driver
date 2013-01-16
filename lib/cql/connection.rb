# encoding: utf-8

require 'socket'
require 'resolv'
require 'resolv-replace'


module Cql
  TimeoutError = Class.new(CqlError)

  class Connection
    def initialize(options={})
      @host = options[:host] || 'localhost'
      @port = options[:port] || 9042
      @timeout = options[:timeout] || 10
      @queue_lock = Mutex.new
    end

    def open
      @socket = connect(@host, @port, @timeout)
      @request_queue = []
      @io_thread = Thread.start(&method(:io_loop))
      self
    rescue Errno::EHOSTUNREACH, SocketError => e
      raise TimeoutError, e.message, e.backtrace
    end

    def close
      @closed = true
    end

    def closed?
      @closed
    end

    def execute(request, &handler)
      @queue_lock.synchronize do
        @request_queue << [request, handler]
      end
      nil
    end

    def execute!(request)
      response = nil
      reader, writer = IO.pipe
      execute(request) do |res|
        response = res
        writer.write(0)
        writer.close
      end
      IO.select([reader])
      response
    end

    private

    def connect(host, port, timeout)
      socket = nil
      exception = nil
      addrinfo = Socket.getaddrinfo(host, port, nil, Socket::Constants::SOCK_STREAM)
      addrinfo.each do |_, port, _, ip, address_family, socket_type|
        sockaddr = Socket.sockaddr_in(port, ip)
        begin
          socket = Socket.new(address_family, socket_type, 0)
          socket.connect_nonblock(sockaddr)
          return socket
        rescue Errno::EINPROGRESS
          IO.select(nil, [socket], nil, timeout)
          begin
            socket.connect_nonblock(sockaddr)
            return socket
          rescue Errno::EISCONN
            return socket
          rescue Errno::EINVAL => e
            exception = e
            socket.close
            next
          rescue SystemCallError => e
            exception = e
            socket.close
            next
          end
        rescue SystemCallError => e
          exception = e
          socket.close
          next
        end
      end
      raise exception
    end

    def io_loop
      Thread.current.abort_on_exception = true

      response_frame = nil
      request_frame_data = nil
      response_handler = nil

      until closed?
        unless response_frame || request_frame_data
          request, handler = @queue_lock.synchronize do
            @request_queue.pop
          end

          if request
            request_frame_data = Cql::RequestFrame.new(request).write('')
            response_frame = Cql::ResponseFrame.new
            response_handler = handler
          end
        end

        readables, writables, _ = IO.select([@socket], [@socket], nil, 1)

        if readables && response_frame && readables.include?(@socket)
          response_frame << @socket.read_nonblock(2**16)
          if response_frame.complete?
            handler.call(response_frame.body)
            response_frame = nil
            # TODO: keep remaning buffer
          end
        end

        if writables && request_frame_data
          bytes_written = @socket.write_nonblock(request_frame_data)
          if bytes_written == request_frame_data.length
            request_frame_data = nil
          else
            request_frame_data.slice!(0, bytes_written)
          end
        end
      end

      @socket.close
    end
  end
end