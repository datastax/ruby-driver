# encoding: utf-8

require 'socket'


module Cql
  module Io
    class SocketHandler
      attr_reader :host, :port, :connection_timeout

      def initialize(host, port, connection_timeout, unblocker, socket_impl=Socket, clock=Time)
        @host = host
        @port = port
        @connection_timeout = connection_timeout
        @unblocker = unblocker
        @socket_impl = socket_impl
        @clock = clock
        @lock = Mutex.new
        @connected = false
        @write_buffer = ByteBuffer.new
        @connected_future = Future.new
      end

      def connect
        begin
          unless @io
            @connection_started_at = @clock.now
            addrinfo = @socket_impl.getaddrinfo(@host, @port, Socket::AF_INET, Socket::SOCK_STREAM)
            _, port, _, ip, address_family, socket_type = addrinfo.first
            @sockaddr = @socket_impl.sockaddr_in(port, ip)
            @io = @socket_impl.new(address_family, socket_type, 0)
          end
          unless connected?
            @io.connect_nonblock(@sockaddr)
            @connected = true
            @connected_future.complete!(self)
          end
        rescue Errno::EISCONN
          @connected = true
          @connected_future.complete!(self)
        rescue Errno::EINPROGRESS, Errno::EALREADY
          if @clock.now - @connection_started_at > @connection_timeout
            close(ConnectionTimeoutError.new("Could not connect to #{@host}:#{@port} within #{@connection_timeout}s"))
          end
        rescue SystemCallError => e
          close(e)
        rescue SocketError => e
          close(e) || closed!(e)
        end
        @connected_future
      end

      def close(cause=nil)
        return false unless @io
        begin
          @io.close
        rescue SystemCallError, IOError
          # nothing to do, the socket was most likely already closed
        end
        closed!(cause)
        true
      end

      def connected?
        @connected
      end

      def connecting?
        !(closed? || connected?)
      end

      def closed?
        @io.nil?
      end

      def writable?
        empty_buffer = @lock.synchronize do
          @write_buffer.empty?
        end
        !(closed? || empty_buffer)
      end

      def on_data(&listener)
        @data_listener = listener
      end

      def on_closed(&listener)
        @closed_listener = listener
      end

      def write(bytes=nil)
        @lock.synchronize do
          if block_given?
            yield @write_buffer
          elsif bytes
            @write_buffer.append(bytes)
          end
        end
        @unblocker.unblock!
      end

      def flush
        if writable?
          @lock.synchronize do
            s = @write_buffer.cheap_peek.dup
            bytes_written = @io.write_nonblock(@write_buffer.cheap_peek)
            @write_buffer.discard(bytes_written)
          end
        end
      rescue => e
        close(e)
      end

      def read
        new_data = @io.read_nonblock(2**16)
        @data_listener.call(new_data) if @data_listener
      rescue => e
        close(e)
      end

      def to_io
        @io
      end

      def to_s
        state = 'inconsistent'
        if connected?
          state = 'connected'
        elsif connecting?
          state = 'connecting'
        elsif closed?
          state = 'closed'
        end
        %(#<#{self.class.name} #{state} #{@host}:#{@port}>)
      end

      private

      def closed!(cause)
        @io = nil
        if cause && !cause.is_a?(IoError)
          cause = ConnectionError.new(cause.message)
        end
        unless connected?
          @connected_future.fail!(cause)
        end
        @connected = false
        if @closed_listener
          @closed_listener.call(cause)
        end
      end
    end
  end
end