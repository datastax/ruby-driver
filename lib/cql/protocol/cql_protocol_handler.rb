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

      def initialize(connection, scheduler, protocol_version, compressor=nil)
        @connection = connection
        @scheduler = scheduler
        @compressor = compressor
        @connection.on_data(&method(:receive_data))
        @connection.on_closed(&method(:socket_closed))
        @promises = Array.new(128) { nil }
        @read_buffer = CqlByteBuffer.new
        @frame_encoder = FrameEncoder.new(protocol_version, @compressor)
        @frame_decoder = FrameDecoder.new(@compressor)
        @current_frame = FrameDecoder::NULL_FRAME
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
        @closed_promise.future.on_value(&listener)
        @closed_promise.future.on_failure(&listener)
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
      # that caused the connection to close, or nil.
      #
      # When `timeout` is specified the future will fail with {Cql::TimeoutError}
      # after that many seconds have passed. If a response arrives after that
      # time it will be lost. If a response never arrives for the request the
      # channel occupied by the request will _not_ be reused.
      #
      # @param [Cql::Protocol::Request] request
      # @param [Float] timeout an optional number of seconds to wait until
      #   failing the request
      # @return [Cql::Future<Cql::Protocol::Response>] a future that resolves to
      #   the response
      def send_request(request, timeout=nil)
        return Future.failed(NotConnectedError.new) if closed?
        promise = RequestPromise.new(request, @frame_encoder)
        id = nil
        @lock.synchronize do
          if (id = next_stream_id)
            @promises[id] = promise
          end
        end
        if id
          @connection.write do |buffer|
            @frame_encoder.encode_frame(request, id, buffer)
          end
        else
          @lock.synchronize do
            promise.encode_frame
            @request_queue_in << promise
          end
        end
        if timeout
          @scheduler.schedule_timer(timeout).on_value do
            promise.time_out!
          end
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

        def initialize(request, frame_encoder)
          @request = request
          @frame_encoder = frame_encoder
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

        def encode_frame
          @frame = @frame_encoder.encode_frame(@request)
        end
      end

      def receive_data(data)
        @read_buffer << data
        @current_frame = @frame_decoder.decode_frame(@read_buffer, @current_frame)
        while @current_frame.complete?
          id = @current_frame.stream_id
          if id == -1
            notify_event_listeners(@current_frame.body)
          else
            complete_request(id, @current_frame.body)
          end
          @current_frame = @frame_decoder.decode_frame(@read_buffer)
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
            @frame_encoder.change_stream_id(id, frame)
            @connection.write(frame)
          else
            break
          end
        end
      end

      def socket_closed(cause)
        request_failure_cause = cause || Io::ConnectionClosedError.new
        promises_to_fail = nil
        @lock.synchronize do
          promises_to_fail = @promises.compact
          promises_to_fail.concat(@request_queue_in)
          promises_to_fail.concat(@request_queue_out)
          @promises.fill(nil)
          @request_queue_in.clear
          @request_queue_out.clear
        end
        promises_to_fail.each do |promise|
          promise.fail(request_failure_cause)
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
