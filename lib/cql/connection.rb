# encoding: utf-8

require 'socket'
require 'resolv'
require 'resolv-replace'


module Cql
  ConnectionError = Class.new(CqlError)

  class Connection
    def initialize(options={})
      @host = options[:host] || 'localhost'
      @port = options[:port] || 9042
      @timeout = options[:timeout] || 10
    end

    def connect
      return if @io_reactor
      @io_reactor = IoReactor.new
      @io_reactor.add_connection(@host, @port, @timeout)
      @reactor_thread = Thread.start do
        Thread.current.abort_on_exception = true
        @io_reactor.run
      end
      self
    end

    def close
      @io_reactor && @io_reactor.close
    end

    def closed?
      @io_reactor && @io_reactor.closed?
    end

    def on_event(&listener)
      @io_reactor.add_event_listener(listener)
    end

    def execute(request, &handler)
      @io_reactor.add_request(request, handler)
    end

    def execute!(request)
      execute(request).get
    end

    private

    class NodeConnection
      def initialize(io)
        @io = io
        @write_buffer = ''
        @read_buffer = ''
        @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        @response_tasks = [nil] * 128
        @event_listeners = []
      end

      def to_io
        @io
      end

      def on_event(&listener)
        @event_listeners << listener
      end

      def next_stream_id
        @response_tasks.each_with_index do |task, index|
          return index if task.nil?
        end
        nil
      end

      def has_capacity?
        !!next_stream_id
      end

      def perform_request(request, future)
        stream_id = next_stream_id
        if stream_id
          Protocol::RequestFrame.new(request, stream_id).write(@write_buffer)
          @response_tasks[stream_id] = future
        else
          @queued_requests << [request, future]
        end
      end

      def handle_read
        new_bytes = @io.read_nonblock(2**16)
        @current_frame << new_bytes
        while @current_frame.complete?
          stream_id = @current_frame.stream_id
          if stream_id == EVENT_STREAM_ID
            @event_listeners.each { |listener| listener.call(@current_frame.body) }
          else
            @response_tasks[stream_id].complete!(@current_frame.body)
            @response_tasks[stream_id] = nil
          end
          @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        end
      end

      def handle_write
        unless @write_buffer.empty?
          bytes_written = @io.write_nonblock(@write_buffer)
          @write_buffer.slice!(0, bytes_written)
        end
      end

      def close
        @io.close
      end

      private

      EVENT_STREAM_ID = -1
    end

    class CommandDispatcher
      def initialize(*args)
        @io, @command_queue, @queue_lock, @node_connections = args
      end

      def to_io
        @io
      end

      def has_capacity?
        false
      end

      def on_event; end

      def handle_read
        requests = []
        if @io.read_nonblock(1)
          while (command = next_command)
            case command.shift
            when :event_listener
              listener = command.shift
              @node_connections.each { |c| c.on_event(&listener) }
            else
              request, future = command
              @node_connections.select(&:has_capacity?).sample.perform_request(request, future)
            end
          end
        end
      end

      def handle_write
      end

      def close
        @io.close
      end

      private

      def next_command
        @queue_lock.synchronize do
          if @node_connections.any?(&:has_capacity?) && @command_queue.size > 0
            return @command_queue.shift
          end
        end
        nil
      end
    end

    class IoReactor
      def initialize
        @lock = Mutex.new
        @streams = []
        @command_queue = []
        @queue_signal_receiver, @queue_signal_sender = IO.pipe
      end

      def close
        @closed = true
      end

      def closed?
        @closed
      end

      def run
        @lock.synchronize do
          @streams << CommandDispatcher.new(@queue_signal_receiver, @command_queue, @lock, @streams)
        end
        @closed = false
        io_loop
      end

      def add_connection(host, port, timeout)
        @lock.synchronize do
          begin
            @streams = @streams + [NodeConnection.new(connect(host, port, timeout))]
          rescue Errno::EHOSTUNREACH, Errno::EBADF, Errno::EINVAL, SystemCallError, SocketError => e
            raise ConnectionError, "Could not connect to #@host:#@port: #{e.message} (#{e.class.name})", e.backtrace
          end
        end
      end

      def add_event_listener(listener)
        command_queue_push(:event_listener, listener)
      end

      def add_request(request, listener)
        future = ResponseFuture.new
        future.on_complete(&listener) if listener
        command_queue_push(:request, request, future)
        future
      end

      private

      PING_BYTE = "\0".freeze

      def command_queue_push(*item)
        @lock.synchronize do
          @command_queue << item
        end
        @queue_signal_sender.write(PING_BYTE)
      end

      def io_loop
        until closed?
          readables, writables, _ = IO.select(@streams, @streams, nil, 1)

          readables.each(&:handle_read)
          writables.each(&:handle_write)
        end
      rescue Errno::ECONNRESET, IOError => e
        close
      ensure
        @streams.each do |stream|
          begin
            stream.close
          rescue IOError
          end
        end
      end

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
    end

    class ResponseFuture
      def initialize
        @listeners = []
      end

      def on_complete(&listener)
        if @response
          listener.call(@response)
        else
          @listeners << listener
        end
      end

      def complete!(response)
        @response = response
        @lock << :ping if @lock
        @listeners.each { |l| l.call(response) }
        @listeners.clear
      end

      def get
        return @response if @response
        @lock = Queue.new
        @lock.pop
        @response
      end
    end
  end
end