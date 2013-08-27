# encoding: utf-8

module Cql
  module Protocol
    # This class wraps a single connection and translates between request/
    # response frames and raw bytes.
    #
    # You send requests with #send_request, and receive responses through the
    # returned future.
    #
    # Instances of this class are thread safe.
    #
    # @example Sending an OPTIONS request
    #   future = protocol_handler.send_request(Cql::Protocol::OptionsRequest.new)
    #   response = future.get
    #   puts "These options are supported: #{response.options}"
    #
    class CqlProtocolHandler
      # @return [String] the current keyspace for the underlying connection
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
        @data = {}
        @lock = Mutex.new
        @closed_future = Future.new
        @keyspace = nil
      end

      # Returns the hostname of the underlying connection
      #
      # @return [String] the hostname
      def host
        @connection.host
      end

      # Returns the port of the underlying connection
      #
      # @return [Integer] the port
      def port
        @connection.port
      end

      # Associate arbitrary data with this protocol handler object. This is
      # useful in situations where additional metadata can be loaded after the
      # connection has been set up, or to keep statistics specific to the
      # connection this protocol handler wraps.
      def []=(key, value)
        @lock.synchronize { @data[key] = value }
      end

      # @see {#[]=}
      # @return the value associated with the key
      def [](key)
        @lock.synchronize { @data[key] }
      end

      # @return [true, false] true if the underlying connection is connected
      def connected?
        @connection.connected?
      end

      # @return [true, false] true if the underlying connection is closed
      def closed?
        @connection.closed?
      end

      # Register to receive notification when the underlying connection has
      # closed. If the connection closed abruptly the error will be passed
      # to the listener, otherwise it will not receive any parameters.
      #
      # @yieldparam error [nil, Error] the error that caused the connection to
      #   close, if any
      def on_closed(&listener)
        @closed_future.on_complete(&listener)
        @closed_future.on_failure(&listener)
      end

      # Register to receive server sent events, like schema changes, nodes going
      # up or down, etc. To actually receive events you also need to send a
      # REGISTER request for the events you wish to receive.
      #
      # @yieldparam event [Cql::Protocol::EventResponse] an event sent by the server
      def on_event(&listener)
        @lock.synchronize do
          @event_listeners << listener
        end
      end

      # Serializes and send a request over the underlying connection.
      #
      # Returns a future that will resolve to the response. When the connection
      # closes the futures of all active requests will be failed with the error
      # that caused the connection to close, or nil
      #
      # @return [Cql::Future<Cql::Protocol::Response>] a future that resolves to
      #   the response
      def send_request(request)
        return Future.failed(NotConnectedError.new) if closed?
        future = Future.new
        id = nil
        @lock.synchronize do
          if (id = next_stream_id)
            @responses[id] = future
          end
        end
        if id
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

      # Closes the underlying connection.
      #
      # @return [Cql::Future] a future that completes when the connection has closed
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
        while true
          id = nil
          request_buffer = nil
          @lock.synchronize do
            if @request_queue_out.any? && (id = next_stream_id)
              request_buffer, future = @request_queue_out.shift
              @responses[id] = future
            end
          end
          if id
            Protocol::Request.change_stream_id(id, request_buffer)
            @connection.write(request_buffer)
          else
            break
          end
        end
      end

      def socket_closed(cause)
        request_failure_cause = cause || Io::ConnectionClosedError.new
        @lock.synchronize do
          @responses.each_with_index do |future, i|
            if future
              @responses[i].fail!(request_failure_cause)
              @responses[i] = nil
            end
          end
          @request_queue_in.each do |_, future|
            future.fail!(request_failure_cause)
          end
          @request_queue_in.clear
          @request_queue_out.each do |_, future|
            future.fail!(request_failure_cause)
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
        @responses.each_with_index do |task, index|
          return index if task.nil?
        end
        nil
      end
    end
  end
end
