# encoding: utf-8

require 'socket'
require 'resolv-replace'


module Cql
  module Io
    class IoReactor
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

      def running?
        @running
      end

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

      def stop
        @running = false
        @stopped_future
      end

      def add_connection(host, port)
        connection = NodeConnection.new(host, port, @connection_timeout)
        future = connection.open
        future.on_failure do
          @lock.synchronize do
            @streams.delete(connection)
          end
        end
        future.on_complete do
          command_queue_push
        end
        @lock.synchronize do
          @streams << connection
        end
        command_queue_push
        future
      end

      def queue_request(request, connection_id=nil)
        future = Future.new
        command_queue_push(:request, request, future, connection_id)
        future
      end

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

    class NodeConnection
      def initialize(*args)
        @host, @port, @connection_timeout = args
        @connected_future = Future.new
        @io = nil
        @addrinfo = nil
        @write_buffer = ''
        @read_buffer = ''
        @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        @response_tasks = [nil] * 128
        @event_listeners = []
      end

      def open
        @connection_started_at = Time.now
        begin
          addrinfo = Socket.getaddrinfo(@host, @port, Socket::AF_INET, Socket::SOCK_STREAM)
          _, port, _, ip, address_family, socket_type = addrinfo.first
          @sockaddr = Socket.sockaddr_in(port, ip)
          @io = Socket.new(address_family, socket_type, 0)
          @io.connect_nonblock(@sockaddr)
        rescue Errno::EINPROGRESS
          # ok
        rescue SystemCallError, SocketError => e
          fail_connection!(e)
        end
        @connected_future
      end

      def connection_id
        self.object_id
      end

      def to_io
        @io
      end

      def on_event(&listener)
        @event_listeners << listener
      end

      def ping
        if @io && connecting? && (Time.now - @connection_started_at > @connection_timeout)
          fail_connection!
        end
      end

      def connected?
        @io && !connecting?
      end

      def has_capacity?
        !!next_stream_id && connected?
      end

      def can_write?
        @io && (!@write_buffer.empty? || connecting?)
      end

      def perform_request(request, future)
        stream_id = next_stream_id
        Protocol::RequestFrame.new(request, stream_id).write(@write_buffer)
        @response_tasks[stream_id] = future
      end

      def handle_read
        new_bytes = @io.read_nonblock(2**16)
        @current_frame << new_bytes
        while @current_frame.complete?
          stream_id = @current_frame.stream_id
          if stream_id == EVENT_STREAM_ID
            @event_listeners.each { |listener| listener.call(@current_frame.body) }
          elsif @response_tasks[stream_id]
            @response_tasks[stream_id].complete!([@current_frame.body, connection_id])
            @response_tasks[stream_id] = nil
          else
            # TODO dropping the request on the floor here, but we didn't send it
          end
          @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        end
      end

      def handle_write
        if connecting?
          handle_connected
        else
          bytes_written = @io.write_nonblock(@write_buffer)
          @write_buffer.slice!(0, bytes_written)
        end
      end

      def close
        if @io
          @io.close
          if connecting?
            succeed_connection!
          end
        end
      end

      def to_s
        state = begin
          if connected? then 'connected'
          elsif connecting? then 'connecting'
          else 'not connected'
          end
        end
        %<NodeConnection(#{@host}:#{@port}, #{state})>
      end

      private

      EVENT_STREAM_ID = -1

      def connecting?
        !@connected_future.complete?
      end

      def handle_connected
        @io.connect_nonblock(@sockaddr)
        succeed_connection!
      rescue Errno::EISCONN
        # ok
        succeed_connection!
      rescue SystemCallError, SocketError => e
        fail_connection!(e)
      end

      def succeed_connection!
        @connected_future.complete!(connection_id)
      end

      def fail_connection!(e=nil)
        message = "Could not connect to #{@host}:#{@port}"
        message << ": #{e.message} (#{e.class.name})" if e
        error = ConnectionError.new(message)
        error.set_backtrace(e.backtrace) if e
        @connected_future.fail!(error)
        @io.close if @io
        @io = nil
      end

      def next_stream_id
        @response_tasks.each_with_index do |task, index|
          return index if task.nil?
        end
        nil
      end
    end

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

      def has_capacity?
        false
      end

      def can_write?
        false
      end

      def on_event; end

      def ping
        if can_deliver_command?
          deliver_commands
        end
      end

      def handle_read
        if @io.read_nonblock(1)
          deliver_commands
        end
      end

      def handle_write
      end

      def close
        @io.close
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
          if can_deliver_command?
            return @command_queue.shift
          end
        end
        nil
      end

      def deliver_commands
        while (command = next_command)
          case command.shift
          when :event_listener
            listener = command.shift
            @node_connections.each { |c| c.on_event(&listener) }
          else
            request, future, connection_id = command
            if connection_id
              connection = @node_connections.find { |c| c.connection_id == connection_id }
              if connection && connection.has_capacity?
                connection.perform_request(request, future)
              elsif connection
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
    end
  end
end