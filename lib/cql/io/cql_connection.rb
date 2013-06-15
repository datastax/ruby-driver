# encoding: utf-8

require 'socket'


module Cql
  module Io
    class CqlConnection
      def initialize(socket_handler)
        @socket_handler = socket_handler
        @socket_handler.on_data(&method(:receive_data))
        @responses = Array.new(128) { nil }
        @read_buffer = ByteBuffer.new
        @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        @request_queue = []
        @event_listeners = []
      end

      def on_event(&handler)
        @event_listeners << handler
      end

      def send_request(request)
        id = next_stream_id
        future = Future.new
        if id
          @responses[id] = future
          @socket_handler.write do |buffer|
            request.encode_frame(id, buffer)
          end
        else
          @request_queue << [request.encode_frame(0), future]
        end
        future
      end

      private

      def receive_data(data)
        @current_frame << data
        while @current_frame.complete?
          id = @current_frame.stream_id
          if id == -1
            @event_listeners.each { |listener| listener.call(@current_frame.body) }
          else
            @responses[id].complete!(@current_frame.body)
            @responses[id] = nil
          end
          @current_frame = Protocol::ResponseFrame.new(@read_buffer)
          flush_request_queue
        end
      end

      def flush_request_queue
        while @request_queue.any? && (id = next_stream_id)
          request_buffer, future = @request_queue.shift
          Protocol::Request.change_stream_id(id, request_buffer)
          @socket_handler.write(request_buffer)
          @responses[id] = future
        end
      end

      def next_stream_id
        @responses.each_with_index do |task, index|
          return index if task.nil?
        end
        nil
      end
    end
  end
end
