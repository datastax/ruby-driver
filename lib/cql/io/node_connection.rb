# encoding: utf-8

require 'socket'


module Cql
  module Io
    # @private
    class NodeConnection
      def initialize(host, port, connection_timeout)
        @socket_handler = SocketHandler.new(host, port, connection_timeout)
        @connected_future = Future.new
        @read_buffer = ByteBuffer.new
        @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        @response_tasks = [nil] * 128
        @event_listeners = Hash.new { |h, k| h[k] = [] }
      end

      def open
        @socket_handler.on_connected(&method(:handle_connected))
        @socket_handler.on_closed(&method(:handle_closed))
        @socket_handler.on_data(&method(:handle_data))
        @socket_handler.open
        @connected_future
      end

      def connection_id
        self.object_id
      end

      def to_io
        @socket_handler.to_io
      end

      def on_event(&listener)
        @event_listeners[:event] << listener
      end

      def on_close(&listener)
        @event_listeners[:close] << listener
      end

      def connected?
        @socket_handler.connected?
      end

      def connecting?
        @socket_handler.connecting?
      end

      def closed?
        @socket_handler.closed?
      end

      def has_capacity?
        !!next_stream_id && connected?
      end

      def can_write?
        @socket_handler.writable? || @socket_handler.connecting?
      end

      def handle_read
        @socket_handler.read
      end

      def perform_request(request, future)
        stream_id = next_stream_id
        @socket_handler.write(request.encode_frame(stream_id))
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

      def handle_write
        if connecting?
          handle_connecting
        else
          @socket_handler.flush
        end
      rescue => e
        @socket_handler.close(e)
      end

      def handle_connecting
        @socket_handler.open
      end

      def close
        @socket_handler.close
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

      def handle_connected
        succeed_connection!
      end

      def handle_closed(cause)
        error = nil
        case cause
        when nil
        when CqlError
          error = cause
        else
          error = IoError.new(cause.message)
          error.set_backtrace(cause.backtrace)
        end
        unless @connected_future.complete? || @connected_future.failed?
          if error
            fail_connection!(error)
          else
            succeed_connection!
          end
        end
        @event_listeners[:close].each { |listener| listener.call(self) }
        @response_tasks.each do |listener|
          listener.fail!(error) if listener
        end
      end

      def handle_data(new_bytes)
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
        @socket_handler.close(e)
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