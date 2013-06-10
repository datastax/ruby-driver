# encoding: utf-8

require 'socket'


module Cql
  module Io
    class SocketHandler
      def initialize(host, port, connection_timeout, socket_impl=Socket, clock=Time)
        @host = host
        @port = port
        @connection_timeout = connection_timeout
        @socket_impl = socket_impl
        @clock = clock
        @connected = false
        @write_buffer = ByteBuffer.new
      end

      def open
        unless @io
          @connection_started_at = @clock.now
          addrinfo = @socket_impl.getaddrinfo(@host, @port, Socket::AF_INET, Socket::SOCK_STREAM)
          _, port, _, ip, address_family, socket_type = addrinfo.first
          @sockaddr = @socket_impl.sockaddr_in(port, ip)
          @io = @socket_impl.new(address_family, socket_type, 0)
        end
        unless connected?
          @io.connect_nonblock(@sockaddr)
          connected!
        end
        true
      rescue Errno::EISCONN
        connected!
        true
      rescue Errno::EINPROGRESS, Errno::EALREADY
        if @clock.now - @connection_started_at > @connection_timeout
          close(ConnectionTimeoutError.new("Could not connect to #{@host}:#{@port} within #{@connection_timeout}s"))
        end
        false
      rescue SystemCallError => e
        close(e)
        false
      rescue SocketError => e
        close(e) || closed!(e)
        false
      end

      def close(cause=nil)
        return false unless @io
        begin
          @io.close
        rescue SystemCallError
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
        !@io
      end

      def writable?
        !(closed? || @write_buffer.empty?)
      end

      def on_data(&listener)
        @data_listener = listener
      end

      def on_connected(&listener)
        @connected_listener = listener
      end

      def on_closed(&listener)
        @closed_listener = listener
      end

      def write(bytes)
        @write_buffer.append(bytes)
      end

      def flush
        if writable?
          bytes_written = @io.write_nonblock(@write_buffer.cheap_peek)
          @write_buffer.discard(bytes_written)
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

      private

      def connected!
        @connected = true
        @connected_listener.call if @connected_listener
      end

      def closed!(cause)
        @io = nil
        @connected = false
        @closed_listener.call(cause) if @closed_listener
      end
    end
  end
end