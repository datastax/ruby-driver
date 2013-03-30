# encoding: utf-8

require 'socket'
require 'resolv-replace'


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
        @streams = []
        @command_queue = []
        @queue_signal_receiver, @queue_signal_sender = IO.pipe
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
            @streams << CommandDispatcher.new(@queue_signal_receiver, @command_queue, @lock, @streams)
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
        future = connection.open
        future.on_failure do
          @lock.synchronize do
            @streams.delete(connection)
          end
        end
        future.on_complete do
          command_queue_push(:connection_established, connection)
        end
        @lock.synchronize do
          @streams << connection
        end
        command_queue_push(:connection_added, connection)
        future
      end

      # Sends a request over a random, or specific connection.
      #
      # @param [Cql::Protocol::RequestBody] request the request to send
      # @param [Object] connection_id the ID of the connection which should be
      #   used to send the request
      # @return [Future<ResultResponse>] a future representing the result of the request
      #
      def queue_request(request, connection_id=nil)
        future = Future.new
        command_queue_push(:request, request, future, connection_id)
        future
      end

      # Registers a listener to receive server sent events.
      #
      # @yieldparam [Cql::Protocol::EventResponse] event the event sent by the server
      #
      def add_event_listener(&listener)
        command_queue_push(:event_listener, listener)
      end

      private

      PING_BYTE = "\0".freeze

      def io_loop
        while running?
          read_ready_streams = @streams.select(&:connected?)
          write_ready_streams = @streams.select(&:can_write?)
          readables, writables, _ = IO.select(read_ready_streams, write_ready_streams, nil, 1)
          readables && readables.each(&:handle_read)
          writables && writables.each(&:handle_write)
          @streams.each(&:ping)
          @streams.reject!(&:closed?)
        end
      ensure
        stop
        @streams.each do |stream|
          begin
            stream.close
          rescue IOError
          end
        end
      end

      def command_queue_push(*item)
        if item && item.any?
          @lock.synchronize do
            @command_queue << item
          end
        end
        @queue_signal_sender.write(PING_BYTE)
      end
    end

    # @private
    class CommandDispatcher
      def initialize(*args)
        @io, @command_queue, @queue_lock, @node_connections = args
      end

      def connection_id
        -1
      end

      def to_io
        @io
      end

      def connected?
        true
      end

      def closed?
        false
      end

      def has_capacity?
        false
      end

      def can_write?
        false
      end

      def on_event; end

      def on_close; end

      def ping
        if can_deliver_command?
          deliver_commands
        else
          prune_directed_requests!
        end
      end

      def handle_read
        if can_deliver_command?
          if @io.read_nonblock(1)
            deliver_commands
          end
        end
      end

      def handle_write
      end

      def close
        @io.close
        @io = nil
      end

      def to_s
        %(CommandDispatcher)
      end

      private

      def can_deliver_command?
        @node_connections.any?(&:has_capacity?) && @command_queue.size > 0
      end

      def next_command
        @queue_lock.synchronize do
          return @command_queue.shift
        end
        nil
      end

      def deliver_commands
        while (command = next_command)
          case command.shift
          when :connection_added
          when :connection_established
            connection = command.shift
            connection.on_close(&method(:connection_closed))
          when :event_listener
            listener = command.shift
            @node_connections.each { |c| c.on_event(&listener) }
          when :request
            request, future, connection_id = command
            if connection_id
              connection = @node_connections.find { |c| c.connection_id == connection_id }
              if connection && connection.connected? && connection.has_capacity?
                connection.perform_request(request, future)
              elsif connection && connection.connected?
                future.fail!(ConnectionBusyError.new("Connection ##{connection_id} is busy"))
              else
                future.fail!(ConnectionNotFoundError.new("Connection ##{connection_id} does not exist"))
              end
            else
              @node_connections.select(&:has_capacity?).sample.perform_request(request, future)
            end
          end
        end
      end

      def prune_directed_requests!
        failing_commands = []
        @queue_lock.synchronize do
          @command_queue.reject! do |command|
            if command.first == :request && command.last && @node_connections.none? { |c| c.connection_id == command.last }
              failing_commands << command
              true
            else
              false
            end
          end
        end
        failing_commands.each do |command|
          _, _, future, id = command
          future.fail!(ConnectionNotFoundError.new("Connection ##{id} no longer exists"))
        end
      end

      def connection_closed(connection)
        prune_directed_requests!
      end
    end
  end
end