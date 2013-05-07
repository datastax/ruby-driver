# encoding: utf-8

require 'socket'
require 'resolv-replace'


module Cql
  module Io
    # @private
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
        @event_listeners = Hash.new { |h, k| h[k] = [] }
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
          # NOTE not connected yet, this is expected
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
        @event_listeners[:event] << listener
      end

      def on_close(&listener)
        @event_listeners[:close] << listener
      end

      def connected?
        @io && !connecting?
      end

      def connecting?
        @io && !(@connected_future.complete? || @connected_future.failed?)
      end

      def closed?
        @io.nil? && !connecting?
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
      rescue => e
        case e
        when CqlError
          error = e
        else
          error = IoError.new(e.message)
          error.set_backtrace(e.backtrace)
        end
        @response_tasks.delete(stream_id)
        future.fail!(error)
      end

      def handle_read
        new_bytes = @io.read_nonblock(2**16)
        @current_frame << new_bytes
        while @current_frame.complete?
          stream_id = @current_frame.stream_id
          if stream_id == EVENT_STREAM_ID
            @event_listeners[:event].each { |listener| listener.call(@current_frame.body) }
          elsif @response_tasks[stream_id]
            @response_tasks[stream_id].complete!([@current_frame.body, connection_id])
            @response_tasks[stream_id] = nil
          else
            # TODO dropping the request on the floor here, but we didn't send it
          end
          @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        end
      rescue => e
        force_close(e)
      end

      def handle_write
        succeed_connection! if connecting?
        if !@write_buffer.empty?
          bytes_written = @io.write_nonblock(@write_buffer)
          @write_buffer.slice!(0, bytes_written)
        end
      rescue => e
        force_close(e)
      end

      def handle_connecting
        if connecting_timed_out?
          fail_connection!(ConnectionTimeoutError.new("Could not connect to #{@host}:#{@port} within #{@connection_timeout}s"))
        else
          @io.connect_nonblock(@sockaddr)
          succeed_connection!
        end
      rescue Errno::EALREADY, Errno::EINPROGRESS
        # NOTE still not connected
      rescue Errno::EISCONN
        succeed_connection!
      rescue SystemCallError, SocketError => e
        fail_connection!(e)
      end

      def close
        if @io
          begin
            @io.close
          rescue SystemCallError
            # NOTE nothing to do, it wasn't open
          end
          if connecting?
            succeed_connection!
          end
          @io = nil
          @event_listeners[:close].each { |listener| listener.call(self) }
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

      def connecting_timed_out?
        (Time.now - @connection_started_at) > @connection_timeout
      end

      def succeed_connection!
        @connected_future.complete!(connection_id)
      end

      def fail_connection!(e)
        case e
        when ConnectionError
          error = e
        else
          message = "Could not connect to #{@host}:#{@port}: #{e.message} (#{e.class.name})"
          error = ConnectionError.new(message)
          error.set_backtrace(e.backtrace)
        end
        @connected_future.fail!(error)
        force_close(error)
      end

      def force_close(e)
        case e
        when CqlError
          error = e
        else
          error = IoError.new(e.message)
          error.set_backtrace(e.backtrace)
        end
        @response_tasks.each do |listener|
          listener.fail!(error) if listener
        end
        close
      end

      def next_stream_id
        @response_tasks.each_with_index do |task, index|
          return index if task.nil?
        end
        nil
      end
    end
  end
end