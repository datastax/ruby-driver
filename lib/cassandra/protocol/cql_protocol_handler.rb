# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

module Cassandra
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
    #   future = protocol_handler.send_request(Cassandra::Protocol::OptionsRequest.new)
    #   response = future.get
    #   puts "These options are supported: #{response.options}"
    class CqlProtocolHandler
      # @return [String] the current keyspace for the underlying connection
      attr_reader :keyspace

      # @return [Exception] outstanding error, from a failed connection.
      attr_reader :error

      # @return [Integer] the version of the protocol to use in communicating with C*.
      attr_reader :protocol_version

      def initialize(connection,
                     scheduler,
                     protocol_version,
                     compressor = nil,
                     heartbeat_interval = 30,
                     idle_timeout = 60,
                     requests_per_connection = 128,
                     custom_type_handlers = {})
        @protocol_version = protocol_version
        @connection = connection
        @scheduler = scheduler
        @compressor = compressor
        @connection.on_data(&method(:receive_data))
        @connection.on_closed(&method(:socket_closed))

        @streams = Array.new(requests_per_connection) {|i| i}

        @promises = {}

        if protocol_version > 3
          @frame_encoder = V4::Encoder.new(@compressor, protocol_version)
          @frame_decoder = V4::Decoder.new(self, @compressor, custom_type_handlers)
        elsif protocol_version > 2
          @frame_encoder = V3::Encoder.new(@compressor, protocol_version)
          @frame_decoder = V3::Decoder.new(self, @compressor)
        else
          @frame_encoder = V1::Encoder.new(@compressor, protocol_version)
          @frame_decoder = V1::Decoder.new(self, @compressor)
        end

        @request_queue_in = []
        @request_queue_out = []
        @event_listeners = []
        @data = {}
        @lock = Mutex.new
        @closed_promise = Ione::Promise.new
        @keyspace = nil
        @heartbeat = nil
        @terminate = nil
        @heartbeat_interval = heartbeat_interval
        @idle_timeout = idle_timeout
        @error = nil
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
        @lock.lock
        @data[key] = value
      ensure
        @lock.unlock
      end

      # @see {#[]=}
      # @return the value associated with the key
      def [](key)
        @lock.lock
        @data[key]
      ensure
        @lock.unlock
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
      # @yieldparam event [Cassandra::Protocol::EventResponse] an event sent by the server
      def on_event(&listener)
        @lock.lock
        @event_listeners += [listener]
      ensure
        @lock.unlock
      end

      # Serializes and send a request over the underlying connection.
      #
      # Returns a future that will resolve to the response. When the connection
      # closes the futures of all active requests will be failed with the error
      # that caused the connection to close, or nil.
      #
      # When `timeout` is specified the future will fail with
      # {Cassandra::Errors::TimeoutError} after that many seconds have passed. If a
      # response arrives after that time it will be lost. If a response never arrives
      # for the request the channel occupied by the request will _not_ be reused.
      #
      # @param [Cassandra::Protocol::Request] request
      # @param [Float] timeout an optional number of seconds to wait until
      #   failing the request
      # @return [Ione::Future<Cassandra::Protocol::Response>] a future that resolves to
      #   the response
      def send_request(request, timeout = nil, with_heartbeat = true)
        return Ione::Future.failed(Errors::IOError.new('Connection closed')) if closed?
        schedule_heartbeat if with_heartbeat
        promise = RequestPromise.new(request, timeout)
        id = nil
        @lock.lock
        begin
          if (id = next_stream_id)
            @promises[id] = promise
          end
        ensure
          @lock.unlock
        end
        if id
          write_request(id, promise)
        else
          @lock.lock
          begin
            @request_queue_in << promise
          ensure
            @lock.unlock
          end
        end
        promise.future
      end

      # Closes the underlying connection.
      #
      # @return [Ione::Future] a future that completes when the connection has closed
      def close(cause = nil)
        if @heartbeat
          @scheduler.cancel_timer(@heartbeat)
          @heartbeat = nil
        end

        if @terminate
          @scheduler.cancel_timer(@terminate)
          @terminate = nil
        end

        @scheduler.schedule_timer(0).on_value do
          @connection.close(cause)
        end

        @closed_promise.future
      end

      def notify_event_listeners(event_response)
        event_listeners = nil
        @lock.lock
        begin
          event_listeners = @event_listeners
          return if event_listeners.empty?
        ensure
          @lock.unlock
        end
        event_listeners.each do |listener|
          listener.call(event_response)
        end
      end

      def complete_request(id, response)
        promise = nil
        @lock.lock
        begin
          promise = @promises.delete(id)
          @streams.unshift(id)
        ensure
          @lock.unlock
        end
        @keyspace = response.keyspace if response.is_a?(Protocol::SetKeyspaceResultResponse)
        if response.is_a?(Protocol::SchemaChangeResultResponse) &&
           response.change == 'DROPPED' &&
           response.keyspace == @keyspace &&
           response.target == Protocol::Constants::SCHEMA_CHANGE_TARGET_KEYSPACE
          @keyspace = nil
        end
        flush_request_queue
        promise.fulfill(response) unless promise.timed_out?
      end

      private

      # @private
      class RequestPromise < Ione::Promise
        extend AttrBoolean

        attr_reader :request, :timeout
        attr_boolean :timed_out

        def initialize(request, timeout)
          @request = request
          @timeout = timeout
          @timed_out = false
          super()
        end

        def time_out!
          unless future.completed?
            @timed_out = true
            # rubocop:disable Style/SignalException
            fail(Errors::TimeoutError.new('Timed out'))
            # rubocop:enable Style/SignalException
          end
        end
      end

      def receive_data(data)
        reschedule_termination
        @frame_decoder << data
      end

      def flush_request_queue
        @lock.lock
        begin
          if @request_queue_out.empty? && !@request_queue_in.empty?
            @request_queue_out = @request_queue_in
            @request_queue_in = []
          end
        ensure
          @lock.unlock
        end
        loop do
          id = nil
          promise = nil
          @lock.lock
          begin
            if @request_queue_out.any? && (id = next_stream_id)
              promise = @request_queue_out.shift
              next if promise.timed_out?
              @promises[id] = promise
            end
          ensure
            @lock.unlock
          end

          break unless id
          write_request(id, promise)
        end
      end

      def write_request(id, request_promise)
        @connection.write do |buffer|
          @frame_encoder.encode(buffer, request_promise.request, id)
        end
        if request_promise.timeout
          @scheduler.schedule_timer(request_promise.timeout).on_value do
            request_promise.time_out!
          end
        end
      end

      def socket_closed(cause)
        if cause
          e = Errors::IOError.new(cause.message)
          e.set_backtrace(cause.backtrace)

          cause = e
        end
        @error = cause

        request_failure_cause = cause || Errors::IOError.new('Connection closed')
        promises_to_fail = nil
        @lock.synchronize do
          @scheduler.cancel_timer(@heartbeat) if @heartbeat
          @scheduler.cancel_timer(@terminate) if @terminate

          @heartbeat = nil
          @terminate = nil

          promises_to_fail = @promises.values
          promises_to_fail.concat(@request_queue_in)
          promises_to_fail.concat(@request_queue_out)
          @promises.clear
          @request_queue_in.clear
          @request_queue_out.clear
        end
        promises_to_fail.each do |promise|
          promise.fail(request_failure_cause) unless promise.timed_out?
        end
        if cause
          @closed_promise.fail(cause)
        else
          @closed_promise.fulfill
        end
      end

      def schedule_heartbeat
        return unless @heartbeat_interval

        timer = nil

        @lock.synchronize do
          @scheduler.cancel_timer(@heartbeat) if @heartbeat && !@heartbeat.resolved?

          @heartbeat = timer = @scheduler.schedule_timer(@heartbeat_interval)
        end

        timer.on_value do
          send_request(HEARTBEAT, nil, false).on_value do
            schedule_heartbeat
          end
        end
      end

      def reschedule_termination
        return unless @idle_timeout

        timer = nil

        @lock.synchronize do
          @scheduler.cancel_timer(@terminate) if @terminate

          @terminate = timer = @scheduler.schedule_timer(@idle_timeout)
        end

        timer.on_value do
          @terminate = nil
          @connection.close(TERMINATED)
        end
      end

      def next_stream_id
        @streams.shift
      end

      HEARTBEAT  = OptionsRequest.new
      TERMINATED = Errors::TimeoutError.new('Terminated due to inactivity')
    end
  end
end
