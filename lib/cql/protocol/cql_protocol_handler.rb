# encoding: utf-8

require 'socket'


module Cql
  module Protocol
    class CqlProtocolHandler
      attr_reader :keyspace

      def initialize(connection)
        @connection = connection
        @connection.on_data(&method(:receive_data))
        @connection.on_closed(&method(:socket_closed))
        @responses = Array.new(128) { nil }
        @read_buffer = ByteBuffer.new
        @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        @request_queue_in = []
        @request_queue_out = []
        @event_listeners = []
        @lock = Mutex.new
        @closed_future = Future.new
        @keyspace = nil
      end

      def connected?
        @connection.connected?
      end

      def connecting?
        @connection.connecting?
      end

      def closed?
        @connection.closed?
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
          @connection.write do |buffer|
            request.encode_frame(id, buffer)
          end
        else
          @lock.synchronize do
            @request_queue_in << [request.encode_frame(0), future]
          end
        end
        future
      end

      def close
        @connection.close
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
        if response.is_a?(Protocol::SetKeyspaceResultResponse)
          @keyspace = response.keyspace
        end
        future.complete!(response)
      end

      def flush_request_queue
        @lock.synchronize do
          if @request_queue_out.empty? && !@request_queue_in.empty?
            @request_queue_out = @request_queue_in
            @request_queue_in = []
          end
        end
        while @request_queue_out.any? && (id = next_stream_id)
          request_buffer, future = @lock.synchronize do
            @request_queue_out.shift
          end
          if request_buffer
            Protocol::Request.change_stream_id(id, request_buffer)
            @connection.write(request_buffer)
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
          @request_queue_in.each do |_, future|
            future.fail!(cause)
          end
          @request_queue_in.clear
          @request_queue_out.each do |_, future|
            future.fail!(cause)
          end
          @request_queue_out.clear
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
