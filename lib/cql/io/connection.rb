# encoding: utf-8

require 'socket'


module Cql
  module Io
    # A wrapper around a socket. Handles connecting to the remote host, reading
    # from and writing to the socket.
    #
    # @private
    class Connection
      attr_reader :host, :port, :connection_timeout

      # @private
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

      # @private
      def connect
        begin
          unless @addrinfos
            @connection_started_at = @clock.now
            @addrinfos = @socket_impl.getaddrinfo(@host, @port, nil, Socket::SOCK_STREAM)
          end
          unless @io
            _, port, _, ip, address_family, socket_type = @addrinfos.shift
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
        rescue Errno::EINVAL => e
          if @addrinfos.empty?
            close(e)
          else
            @io = nil
            retry
          end
        rescue SystemCallError => e
          close(e)
        rescue SocketError => e
          close(e) || closed!(e)
        end
        @connected_future
      end

      # Closes the connection
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

      # @private
      def connecting?
        !(closed? || connected?)
      end

      # Returns true if the connection is connected
      def connected?
        @connected
      end

      # Returns true if the connection is closed
      def closed?
        @io.nil?
      end

      # @private
      def writable?
        empty_buffer = @lock.synchronize do
          @write_buffer.empty?
        end
        !(closed? || empty_buffer)
      end

      # Register to receive notifications when new data is read from the socket.
      #
      # You should only call this method in your protocol handler constructor.
      #
      # Only one callback can be registered, if you register multiple times only
      # the last one will receive notifications. This is not meant as a general
      # event system, it's just for protocol handlers to receive data from their
      # connection. If you want multiple listeners you need to implement that
      # yourself in your protocol handler.
      #
      # It is very important that you don't do any heavy lifting in the callback
      # since it is called from the IO reactor thread, and as long as the
      # callback is working the reactor can't handle any IO and no other
      # callbacks can be called.
      #
      # Errors raised by the callback will be ignored.
      #
      # @yield [String] the new data
      #
      def on_data(&listener)
        @data_listener = listener
      end

      # Register to receive a notification when the socket is closed, both for
      # expected and unexpected reasons.
      #
      # You shoud only call this method in your protocol handler constructor.
      #
      # Only one callback can be registered, if you register multiple times only
      # the last one will receive notifications. This is not meant as a general
      # event system, it's just for protocol handlers to be notified of the
      # connection closing. If you want multiple listeners you need to implement
      # that yourself in your protocol handler.
      #
      # Errors raised by the callback will be ignored.
      #
      # @yield [error, nil] the error that caused the socket to close, or nil if
      #   the socket closed with #close
      #
      def on_closed(&listener)
        @closed_listener = listener
      end

      # Write bytes to the socket.
      #
      # You can either pass in bytes (as a string or as a `ByteBuffer`), or you
      # can use the block form of this method to get access to the connection's
      # internal buffer.
      # 
      # @yieldparam buffer [Cql::ByteBuffer] the connection's internal buffer
      # @param bytes [String, Cql::ByteBuffer] the data to write to the socket
      #
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

      # @private
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

      # @private
      def read
        new_data = @io.read_nonblock(2**16)
        @data_listener.call(new_data) if @data_listener
      rescue => e
        close(e)
      end

      # @private
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