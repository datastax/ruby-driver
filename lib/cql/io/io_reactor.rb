# encoding: utf-8

module Cql
  module Io
    ReactorError = Class.new(IoError)

    class IoReactor
      def initialize(connection_factory, options={})
        @connection_factory = connection_factory
        @unblocker = Unblocker.new
        @io_loop = IoLoopBody.new(options)
        @io_loop.add_socket(@unblocker)
        @running = false
        @stopped = false
        @started_future = Future.new
        @stopped_future = Future.new
        @lock = Mutex.new
      end

      def on_error(&listener)
        @stopped_future.on_failure(&listener)
      end

      def running?
        @running
      end

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
              @stopped_future.complete!
            end
          end
        end
        @started_future
      end

      def stop
        @stopped = true
        @stopped_future
      end

      def connect(host, port, timeout)
        socket_handler = SocketHandler.new(host, port, timeout)
        f = socket_handler.connect
        connection = @connection_factory.new(socket_handler)
        @lock.synchronize do
          @io_loop.add_socket(socket_handler)
        end
        @unblocker.unblock!
        f.map { connection }
      end
    end

    class Unblocker
      def initialize
        @out, @in = IO.pipe
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
        @in.write(PING_BYTE)
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

    class IoLoopBody
      def initialize(options={})
        @selector = options[:selector] || IO
        @lock = Mutex.new
        @sockets = []
      end

      def add_socket(socket)
        @lock.synchronize do
          @sockets << socket
        end
      end

      def close_sockets
        @lock.synchronize do
          @sockets.each do |s|
            begin
              s.close unless s.closed?
            rescue
              # the socket had most likely already closed due to an error
            end
          end
        end
      end

      def tick(timeout=1)
        readables, writables, connecting = [], [], []
        @lock.synchronize do
          @sockets.reject! { |s| s.closed? }
          @sockets.each do |s|
            readables << s if s.connected?
            writables << s if s.connecting? || s.writable?
            connecting << s if s.connecting?
          end
        end
        r, w, _ = @selector.select(readables, writables, nil, timeout)
        connecting.each(&:connect)
        r && r.each(&:read)
        w && w.each(&:flush)
      end
    end
  end
end