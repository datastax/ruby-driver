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
    # connections you open. There shouldn't be any problems handling hundreds of
    # connections if needed. All operations are thread safe, but you should take
    # great care when in your protocol handlers to make sure that they don't
    # do too much work in their data handling callbacks, since those will be
    # run in the reactor thread, and every cycle you use there is a cycle which
    # can't be used to handle IO.
    #
    # The IO reactor is completely protocol agnostic, and it's up to the
    # specified protocol handler factory to create objects that can interpret
    # the bytes received from remote hosts, and to send the correct commands
    # back. The way this works is that when you create an IO reactor you provide
    # a factory that can create protocol handler objects (this factory is most
    # of the time just class, but it could potentially be any object that
    # responds to #new). When you #connect a new protocol handler instance is
    # created and passed a connection. The protocol handler can then register to
    # receive data that arrives over the socket, and it can write data to the
    # socket. It can also register to be notified when the socket is closed, or
    # it can itself close the socket.
    #
    # @example A protocol handler that processes whole lines
    #
    #   class LineProtocolHandler
    #     def initialize(connection, scheduler)
    #       @connection = connection
    #       # register a listener method for new data, this must be done in the
    #       # in the constructor, and only one listener can be registered
    #       @connection.on_data(&method(:process_data))
    #       @buffer = ''
    #     end
    #
    #     def process_data(new_data)
    #       # in this fictional protocol we want to process whole lines, so we
    #       # append new data to our buffer and then loop as long as there is
    #       # a newline in the buffer, everything up until a newline is a
    #       # complete line
    #       @buffer << new_data
    #       while newline_index = @buffer.index("\n")
    #         line = @buffer.slice!(0, newline_index + 1)
    #         line.chomp!
    #         # Now do something interesting with the line, but remember that
    #         # while you're in the data listener method you're executing in the
    #         # IO reactor thread so you're blocking the reactor from doing
    #         # other IO work. You should not do any heavy lifting here, but
    #         # instead hand off the data to your application's other threads.
    #         # One way of doing that is to create a Cql::Future in the method
    #         # that sends the request, and then complete the future in this
    #         # method. How you keep track of which future belongs to which
    #         # reply is very protocol dependent so you'll have to figure that
    #         # out yourself.
    #       end
    #     end
    #
    #     def send_request(command_string)
    #       # This example primarily shows how to implement a data listener
    #       # method, but this is how you write data to the connection. The
    #       # method can be called anything, it doesn't have to be #send_request
    #       @connection.write(command_string)
    #       # The connection object itself is threadsafe, but to create any
    #       # interesting protocol you probably need to set up some state for
    #       # each request so that you know which request to complete when you
    #       # get data back.
    #     end
    #   end
    #
    # See {Cql::Protocol::CqlProtocolHandler} for an example of how the CQL
    # protocol is implemented, and there is an integration tests that implements
    # the Redis protocol that you can look at too.
    #
    # @private
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
        @clock = options[:clock] || Time
        @unblocker = Unblocker.new
        @io_loop = IoLoopBody.new(options)
        @io_loop.add_socket(@unblocker)
        @running = false
        @stopped = false
        @started_promise = Promise.new
        @stopped_promise = Promise.new
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
        @stopped_promise.future.on_failure(&listener)
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
          return @started_promise.future if @running
          @running = true
        end
        Thread.start do
          @started_promise.fulfill(self)
          begin
            @io_loop.tick until @stopped
          ensure
            @io_loop.close_sockets
            @io_loop.cancel_timers
            @running = false
            if $!
              @stopped_promise.fail($!)
            else
              @stopped_promise.fulfill(self)
            end
          end
        end
        @started_promise.future
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
        @stopped_promise.future
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
        connection = Connection.new(host, port, timeout, @unblocker, @clock)
        f = connection.connect
        protocol_handler = @protocol_handler_factory.new(connection, self)
        @io_loop.add_socket(connection)
        @unblocker.unblock!
        f.map { protocol_handler }
      end

      # Returns a future that completes after the specified number of seconds.
      #
      # @param timeout [Float] the number of seconds to wait until the returned
      #   future is completed
      # @return [Cql::Future] a future that completes when the timer expires
      #
      def schedule_timer(timeout)
        @io_loop.schedule_timer(timeout)
      end

      def to_s
        @io_loop.to_s
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
        @clock = options[:clock] || Time
        @lock = Mutex.new
        @sockets = []
        @timers = []
      end

      def add_socket(socket)
        @lock.synchronize do
          sockets = @sockets.reject { |s| s.closed? }
          sockets << socket
          @sockets = sockets
        end
      end

      def schedule_timer(timeout, promise=Promise.new)
        @lock.synchronize do
          timers = @timers.reject { |pair| pair[1].nil? }
          timers << [@clock.now + timeout, promise]
          @timers = timers
        end
        promise.future
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

      def cancel_timers
        @timers.each do |pair|
          if pair[1]
            pair[1].fail(CancelledError.new)
            pair[1] = nil
          end
        end
      end

      def tick(timeout=1)
        check_sockets!(timeout)
        check_timers!
      end

      def to_s
        %(#<#{IoReactor.name} @connections=[#{@sockets.map(&:to_s).join(', ')}]>)
      end

      private

      def check_sockets!(timeout)
        readables, writables, connecting = [], [], []
        sockets = @sockets
        sockets.each do |s|
          next if s.closed?
          readables << s if s.connected?
          writables << s if s.connecting? || s.writable?
          connecting << s if s.connecting?
        end
        r, w, _ = @selector.select(readables, writables, nil, timeout)
        connecting.each(&:connect)
        r && r.each(&:read)
        w && w.each(&:flush)
      end

      def check_timers!
        timers = @timers
        timers.each do |pair|
          if pair[1] && pair[0] <= @clock.now
            pair[1].fulfill
            pair[1] = nil
          end
        end
      end
    end
  end
end