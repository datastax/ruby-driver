# encoding: utf-8

require 'socket'
require 'resolv'
require 'resolv-replace'
require 'thread'


module Cql
  TimeoutError = Class.new(CqlError)

  class Connection
    def initialize(options={})
      @host = options[:host] || 'localhost'
      @port = options[:port] || 9042
      @timeout = options[:timeout] || 10
    end

    def open
      @socket = connect(@host, @port, @timeout)
      @request_queue = Queue.new
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
      @request_queue << [request, handler]
      nil
    end

    def execute!(request)
      q = Queue.new
      execute(request) { |response| q << response }
      q.pop
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
      until closed?
        request, handler = @request_queue.pop
        Cql::RequestFrame.new(request).write(@socket)
        response_frame = Cql::ResponseFrame.new
        until response_frame.complete?
          IO.select([@socket])
          response_frame << @socket.read_nonblock(2**16)
        end
        handler.call(response_frame.body)
      end
      @socket.close
    end
  end
end