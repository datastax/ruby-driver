# encoding: utf-8

module Cql
  module Io
    # An instance of IO reactor manages the connections used by a client.
    #
    # The reactor starts a thread in which all IO is performed. The IO reactor
    # instances are thread safe.
    #
    class IoReactor
      #
      # @param [Hash] options
      # @option options [Integer] :connection_timeout (5) Max time to wait for a
      #   connection, in seconds
      #
      def initialize(options={})
        @connection_timeout = options[:connection_timeout] || 5
        @lock = Mutex.new
        @command_queue = []
        @unblocker = UnblockerConnection.new(*IO.pipe)
        @connections = [@unblocker]
        @started_future = Future.new
        @stopped_future = Future.new
        @running = false
      end

      # Returns whether or not the reactor is running
      #
      def running?
        @running
      end

      # Starts the reactor.
      #
      # Calling this method when the reactor is connecting or is connected has
      # no effect.
      #
      # @return [Future<nil>] a future which completes when the reactor has started
      #
      def start
        @lock.synchronize do
          unless @running
            @running = true
            @reactor_thread = Thread.start do
              begin
                @started_future.complete!
                io_loop
                @stopped_future.complete!
              rescue => e
                @stopped_future.fail!(e)
                raise
              end
            end
          end
        end
        @started_future
      end

      # Stops the reactor.
      #
      # Calling this method when the reactor is stopping or has stopped has
      # no effect.
      #
      # @return [Future<nil>] a future which completes when the reactor has stopped
      #
      def stop
        @running = false
        command_queue_push(nil)
        @stopped_future
      end

      # Establish a new connection.
      #
      # @param [String] host The hostname to connect to
      # @param [Integer] port The port to connect to
      # @return [Future<Object>] a future representing the ID of the newly
      #   established connection, or connection error if the connection fails.
      #
      def add_connection(host, port)
        connection = NodeConnection.new(host, port, @connection_timeout)
        connection.on_close do
          @lock.synchronize do
            @connections.delete(connection)
          end
        end
        f = connection.open
        @lock.synchronize do
          @connections << connection
        end
        command_queue_push(nil)
        f
      end

      # Sends a request over a random, or specific connection.
      #
      # @param [Cql::Protocol::RequestBody] request the request to send
      # @param [Object] connection_id the ID of the connection which should be
      #   used to send the request
      # @return [Future<ResultResponse>] a future representing the result of the request
      #
      def queue_request(request, connection_id=nil)
        command = connection_id ? TargetedRequestCommand.new(request, connection_id) : RequestCommand.new(request)
        command_queue_push(command)
        command.future
      end

      # Registers a listener to receive server sent events.
      #
      # @yieldparam [Cql::Protocol::EventResponse] event the event sent by the server
      #
      def add_event_listener(&listener)
        command_queue_push(EventListenerCommand.new(listener))
      end

      private

      def io_loop
        while running?
          read_ready_streams = @connections.select(&:connected?)
          write_ready_streams = @connections.select(&:can_write?)
          readables, writables, _ = IO.select(read_ready_streams, write_ready_streams, nil, 1)
          readables && readables.each(&:handle_read)
          writables && writables.each(&:handle_write)
          @connections.select(&:connecting?).each(&:handle_connecting)
          perform_queued_commands if running?
        end
      ensure
        stop
        @connections.dup.each do |connection|
          begin
            connection.close
          rescue IOError => e
          end
        end
      end

      def command_queue_push(command)
        if command
          @lock.synchronize do
            @command_queue << command
          end
        end
        @unblocker.unblock!
      end

      def perform_queued_commands
        @lock.synchronize do
          unexecuted_commands = []
          while (command = @command_queue.shift)
            case command
            when EventListenerCommand
              @connections.each do |connection|
                connection.on_event(&command.listener)
              end
            when TargetedRequestCommand
              connection = @connections.find { |c| c.connection_id == command.connection_id }
              if connection && connection.connected? && connection.has_capacity?
                connection.perform_request(command.request, command.future)
              elsif connection && connection.connected?
                command.future.fail!(ConnectionBusyError.new("Connection ##{command.connection_id} is busy"))
              else
                command.future.fail!(ConnectionNotFoundError.new("Connection ##{command.connection_id} does not exist"))
              end
            when RequestCommand
              connection = @connections.select(&:has_capacity?).sample
              if connection
                connection.perform_request(command.request, command.future)
              else
                unexecuted_commands << command
              end
            end
          end
          @command_queue.unshift(*unexecuted_commands) if unexecuted_commands.any?
        end
      end
    end

    class EventListenerCommand
      attr_reader :listener

      def initialize(listener)
        @listener = listener
      end
    end

    class RequestCommand
      attr_reader :future, :request

      def initialize(request)
        @request = request
        @future = Future.new
      end
    end

    class TargetedRequestCommand < RequestCommand
      attr_reader :connection_id

      def initialize(request, connection_id)
        super(request)
        @connection_id = connection_id
      end
    end

    class UnblockerConnection
      def initialize(*args)
        @out, @in = args
      end

      def unblock!
        @in.write(PING_BYTE)
      end

      def to_io
        @out
      end

      def close
      end

      def on_event; end

      def on_close; end

      def connection_id
        -1
      end

      def connected?
        true
      end

      def connecting?
        false
      end

      def can_write?
        false
      end

      def has_capacity?
        false
      end

      def handle_read
        @out.read_nonblock(2**16)
      end

      private

      PING_BYTE = "\0".freeze
    end
  end
end