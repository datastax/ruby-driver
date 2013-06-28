# encoding: utf-8

module Cql
  module Io
    ReactorError = Class.new(IoError)

    # An IO reactor takes care of all the IO for a client. It handles opening
    # new connections, and making sure that connections that have data to send
    # flush to the network, and connections that have data coming in read that
    # data and delegate it to their protocol handlers.
    #
    # All IO is done in a single background thread, regardless of how many
    # connections you open. It shouldn't have any problems handling hundreds of
    # connections if needed. All operations are thread safe. You should take
    # great care when in your protocol handlers to make sure that they don't
    # do too much work in their data handling callbacks, since those will be
    # run in the reactor thread, and every cycle you use there is a cycle which
    # can't be used to handle IO.
    #
    # The IO reactor is completely protocol agnostic, and it's up to the
    # specified protocol handler factory to create objects that can interpret
    # the bytes received from remote hosts, and to send the correct commands
    # back.
    #
    class IoReactor
      # Initializes a new IO reactor.
      #
      # @param protocol_handler_factory [Object] a class that will be used
      #   create the protocol handler objects returned by {#connect}
      # @param options [Hash] only used to inject behaviour during tests
      #
      def initialize(protocol_handler_factory, options={})
        @protocol_handler_factory = protocol_handler_factory
        @unblocker = Unblocker.new
        @io_loop = IoLoopBody.new(options)
        @io_loop.add_socket(@unblocker)
        @running = false
        @stopped = false
        @started_future = Future.new
        @stopped_future = Future.new
        @lock = Mutex.new
      end

      # Register to receive notifications when the reactor shuts down because
      # on an irrecoverable error.
      #
      # The listener block will be called in the reactor thread. Any errors that
      # it raises will be ignored.
      #
      # @yield [error] the error that cause the reactor to stop
      #
      def on_error(&listener)
        @stopped_future.on_failure(&listener)
      end

      # Returns true as long as the reactor is running. It will be true even
      # after #stop has been called, but false when the future returned by
      # #stop completes.
      #
      def running?
        @running
      end

      # Starts the reactor. This will spawn a background thread that will manage
      # all connections.
      #
      # This method is asynchronous and returns a future which completes when
      # the reactor has started.
      #
      # @return [Cql::Future] a future that will resolve to the reactor itself
      def start
        @lock.synchronize do
          raise ReactorError, 'Cannot start a stopped IO reactor' if @stopped
          return @started_future if @running
          @running = true
        end
        Thread.start do
          @started_future.complete!(self)
          begin
            @io_loop.tick until @stopped
          ensure
            @running = false
            @io_loop.close_sockets
            if $!
              @stopped_future.fail!($!)
            else
              @stopped_future.complete!(self)
            end
          end
        end
        @started_future
      end

      # Stops the reactor.
      #
      # This method is asynchronous and returns a future which completes when
      # the reactor has completely stopped, or fails with an error if the reactor
      # stops or has already stopped because of a failure.
      #
      # @return [Cql::Future] a future that will resolve to the reactor itself
      #
      def stop
        @stopped = true
        @stopped_future
      end

      # Opens a connection to the specified host and port.
      #
      # This method is asynchronous and returns a future which completes when
      # the connection has been established, or fails if the connection cannot
      # be established for some reason (the connection takes longer than the
      # specified timeout, the remote host cannot be found, etc.).
      #
      # The object returned in the future will be an instance of the protocol
      # handler class you passed to {#initialize}.
      #
      # @param host [String] the host to connect to
      # @param port [Integer] the port to connect to
      # @param timeout [Numeric] the number of seconds to wait for a connection
      #   before failing
      # @return [Cql::Future] a future that will resolve to a protocol handler
      #   object that will be your interface to interact with the connection
      #
      def connect(host, port, timeout)
        socket_handler = SocketHandler.new(host, port, timeout, @unblocker)
        f = socket_handler.connect
        connection = @protocol_handler_factory.new(socket_handler)
        @io_loop.add_socket(socket_handler)
        @unblocker.unblock!
        f.map { connection }
      end
    end

    # @private
    class Unblocker
      def initialize
        @out, @in = IO.pipe
        @lock = Mutex.new
      end

      def connected?
        true
      end

      def connecting?
        false
      end

      def writable?
        false
      end

      def closed?
        @in.nil?
      end

      def unblock!
        @lock.synchronize do
          @in.write(PING_BYTE)
        end
      end

      def read
        @out.read_nonblock(2**16)
      end

      def close
        @in.close
        @out.close
        @in = nil
        @out = nil
      end

      def to_io
        @out
      end

      def to_s
        %(#<#{self.class.name}>)
      end

      private

      PING_BYTE = "\0".freeze
    end

    # @private
    class IoLoopBody
      def initialize(options={})
        @selector = options[:selector] || IO
        @lock = Mutex.new
        @sockets = []
      end

      def add_socket(socket)
        @lock.synchronize do
          sockets = @sockets.dup
          sockets << socket
          @sockets = sockets
        end
      end

      def close_sockets
        @sockets.each do |s|
          begin
            s.close unless s.closed?
          rescue
            # the socket had most likely already closed due to an error
          end
        end
      end

      def tick(timeout=1)
        readables, writables, connecting = [], [], []
        sockets = @sockets
        sockets.reject! { |s| s.closed? }
        sockets.each do |s|
          readables << s if s.connected?
          writables << s if s.connecting? || s.writable?
          connecting << s if s.connecting?
        end
        r, w, _ = @selector.select(readables, writables, nil, timeout)
        connecting.each(&:connect)
        r && r.each(&:read)
        w && w.each(&:flush)
      end
    end
  end
end