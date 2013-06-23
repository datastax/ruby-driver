# encoding: utf-8

require 'socket'


module Cql
  module Io
    class CqlConnection
      def initialize(socket_handler)
        @socket_handler = socket_handler
        @socket_handler.on_data(&method(:receive_data))
        @socket_handler.on_closed(&method(:socket_closed))
        @responses = Array.new(128) { nil }
        @read_buffer = ByteBuffer.new
        @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        @request_queue = []
        @event_listeners = []
        @lock = Mutex.new
        @closed_future = Future.new
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

      def on_closed(&listener)
        @closed_future.on_complete(&listener)
        @closed_future.on_failure(&listener)
      end

      def on_event(&listener)
        @lock.synchronize do
          @event_listeners << listener
        end
      end

      def send_request(request)
        return Future.failed(NotConnectedError.new) if closed?
        id = next_stream_id
        future = Future.new
        if id
          @lock.synchronize do
            @responses[id] = future
          end
          @socket_handler.write do |buffer|
            request.encode_frame(id, buffer)
          end
        else
          @lock.synchronize do
            @request_queue << [request.encode_frame(0), future]
          end
        end
        future
      end

      def close
        @socket_handler.close
        @closed_future
      end

      private

      def receive_data(data)
        @current_frame << data
        while @current_frame.complete?
          id = @current_frame.stream_id
          if id == -1
            notify_event_listeners(@current_frame.body)
          else
            complete_request(id, @current_frame.body)
          end
          @current_frame = Protocol::ResponseFrame.new(@read_buffer)
          flush_request_queue
        end
      end

      def notify_event_listeners(event_response)
        return if @event_listeners.empty?
        @lock.synchronize do
          @event_listeners.each do |listener|
            listener.call(@current_frame.body) rescue nil
          end
        end
      end

      def complete_request(id, response)
        future = @lock.synchronize do
          future = @responses[id]
          @responses[id] = nil
          future
        end
        future.complete!(response)
      end

      def flush_request_queue
        while @request_queue.any? && (id = next_stream_id)
          request_buffer, future = @lock.synchronize do
            @request_queue.shift
          end
          if request_buffer
            Protocol::Request.change_stream_id(id, request_buffer)
            @socket_handler.write(request_buffer)
            @responses[id] = future
          end
        end
      end

      def socket_closed(cause)
        @lock.synchronize do
          @responses.each_with_index do |future, i|
            if future
              @responses[i].fail!(cause)
              @responses[i] = nil
            end
          end
          @request_queue.each do |_, future|
            future.fail!(cause)
          end
          @request_queue.clear
          if cause
            @closed_future.fail!(cause)
          else
            @closed_future.complete!
          end
        end
      end

      def next_stream_id
        @lock.synchronize do
          @responses.each_with_index do |task, index|
            return index if task.nil?
          end
        end
        nil
      end
    end
  end
end
