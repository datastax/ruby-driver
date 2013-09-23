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

      def initialize(connection, scheduler)
        @connection = connection
        @scheduler = scheduler
        @connection.on_data(&method(:receive_data))
        @connection.on_closed(&method(:socket_closed))
        @promises = Array.new(128) { nil }
        @read_buffer = ByteBuffer.new
        @current_frame = Protocol::ResponseFrame.new(@read_buffer)
        @request_queue_in = []
        @request_queue_out = []
        @event_listeners = []
        @data = {}
        @lock = Mutex.new
        @closed_promise = Promise.new
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
        @closed_promise.future.on_complete(&listener)
      end

      # Register to receive server sent events, like schema changes, nodes going
      # up or down, etc. To actually receive events you also need to send a
      # REGISTER request for the events you wish to receive.
      #
      # @yieldparam event [Cql::Protocol::EventResponse] an event sent by the server
      def on_event(&listener)
        @lock.synchronize do
          @event_listeners += [listener]
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
        promise = RequestPromise.new(request)
        id = nil
        @lock.synchronize do
          if (id = next_stream_id)
            @promises[id] = promise
          end
        end
        if id
          @connection.write do |buffer|
            request.encode_frame(id, buffer)
          end
        else
          @lock.synchronize do
            promise.encode_frame!
            @request_queue_in << promise
          end
        end
        @scheduler.schedule_timer(5).on_value do
          promise.time_out!
        end
        promise.future
      end

      # Closes the underlying connection.
      #
      # @return [Cql::Future] a future that completes when the connection has closed
      def close
        @connection.close
        @closed_promise.future
      end

      private

      # @private
      class RequestPromise < Promise
        attr_reader :request, :frame

        def initialize(request)
          @request = request
          @timed_out = false
          super()
        end

        def timed_out?
          @timed_out
        end

        def time_out!
          unless future.completed?
            @timed_out = true
            fail(TimeoutError.new)
          end
        end

        def encode_frame!
          @frame = @request.encode_frame(0)
        end
      end

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
        event_listeners = nil
        @lock.synchronize do
          event_listeners = @event_listeners
          return if event_listeners.empty?
        end
        event_listeners.each do |listener|
          listener.call(@current_frame.body) rescue nil
        end
      end

      def complete_request(id, response)
        promise = @lock.synchronize do
          promise = @promises[id]
          @promises[id] = nil
          promise
        end
        if response.is_a?(Protocol::SetKeyspaceResultResponse)
          @keyspace = response.keyspace
        end
        unless promise.timed_out?
          promise.fulfill(response)
        end
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
          frame = nil
          @lock.synchronize do
            if @request_queue_out.any? && (id = next_stream_id)
              promise = @request_queue_out.shift
              if promise.timed_out?
                id = nil
              else
                frame = promise.frame
                @promises[id] = promise
              end
            end
          end
          if id
            Protocol::Request.change_stream_id(id, frame)
            @connection.write(frame)
          else
            break
          end
        end
      end

      def socket_closed(cause)
        request_failure_cause = cause || Io::ConnectionClosedError.new
        @lock.synchronize do
          @promises.each_with_index do |promise, i|
            if promise
              @promises[i].fail(request_failure_cause)
              @promises[i] = nil
            end
          end
          @request_queue_in.each do |promise|
            promise.fail(request_failure_cause)
          end
          @request_queue_in.clear
          @request_queue_out.each do |promise|
            promise.fail(request_failure_cause)
          end
          @request_queue_out.clear
        end
        if cause
          @closed_promise.fail(cause)
        else
          @closed_promise.fulfill
        end
      end

      def next_stream_id
        @promises.each_with_index do |task, index|
          return index if task.nil?
        end
        nil
      end
    end
  end
end
