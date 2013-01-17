# encoding: utf-8

require 'socket'
require 'resolv'
require 'resolv-replace'


module Cql
  TimeoutError = Class.new(CqlError)

  class Connection
    def initialize(options={})
      @host = options[:host] || 'localhost'
      @port = options[:port] || 9042
      @timeout = options[:timeout] || 10
    end

    def open
      return if @io_thread
      @queue_lock = Mutex.new
      @queue_signal_receiver, @queue_signal_sender = IO.pipe
      @socket = connect(@host, @port, @timeout)
      @request_queue = []
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
      future = ResponseFuture.new
      future.on_complete(&handler) if handler
      @queue_lock.synchronize do
        @request_queue << [request, future]
        @queue_signal_sender.write(0)
      end
      future
    end

    def execute!(request)
      execute(request).get
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

    class NodeConnection
      def initialize(io)
        @io = io
        @write_buffer = ''
        @read_buffer = ''
        @current_frame = ResponseFrame.new(@read_buffer)
        @response_tasks = [nil] * 128
        @queued_requests = []
      end

      def to_io
        @io
      end

      def next_stream_id
        @response_tasks.each_with_index do |task, index|
          return index unless task
        end
        nil
      end

      def check_request_queue!
        while has_capacity? && @queued_requests.any?
          perform_request(*@queued_requests.shift)
        end
      end

      def has_capacity?
        0 < @queued_requests.count(&:nil?)
      end

      def perform_request(request, future)
        stream_id = next_stream_id
        if stream_id
          RequestFrame.new(request, stream_id).write(@write_buffer)
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
          @response_tasks[stream_id].complete!(@current_frame.body)
          @response_tasks[stream_id] = nil
          @current_frame = ResponseFrame.new(@read_buffer)
          check_request_queue!
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
    end

    class QueueSignalListener
      def initialize(*args)
        @io, @request_queue, @queue_lock, @node_connections = args
      end

      def to_io
        @io
      end

      def handle_read
        request, future = @queue_lock.synchronize do
          @io.read(1)
          @request_queue.pop
        end
        if request
          @node_connections.sample.perform_request(request, future)
        end
      end

      def handle_write
      end

      def close
        @io.close
      end
    end

    def io_loop
      Thread.current.abort_on_exception = true

      node_connections = [NodeConnection.new(@socket)]
      queue_signal_listener = QueueSignalListener.new(@queue_signal_receiver, @request_queue, @queue_lock, node_connections)
      streams = node_connections + [queue_signal_listener]

      until closed?
        readables, writables, _ = IO.select(streams, streams, nil, 1)

        readables.each(&:handle_read)
        writables.each(&:handle_write)
      end

      streams.each(&:close)
    rescue => e
      $stderr.puts("ERROR: #{e.message} (#{e.class.name})")
      raise
    end

    class ResponseFuture
      def initialize
        @lock = Queue.new
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
        @lock << :ping
        @listeners.each { |l| l.call(response) }
        @listeners.clear
      end

      def get
        return @response if @response
        @lock.pop
        @response
      end
    end
  end
end