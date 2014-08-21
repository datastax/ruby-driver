# encoding: utf-8

# Copyright 2013-2014 DataStax, Inc.
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

module Cql
  # A Future represents a result of asynchronous execution. It can be used to
  # block until a value is available or an error has happened, or register a
  # listener to be notified whenever the execution is complete.
  class Future
    # a Future listener to be passed to {Cql::Future#add_listener}
    #
    # @note Listener methods can be called from application if a future has
    #   been resolved or failed by the time the listener is registered; or from
    #   background thread if it is resolved/failed after the listener has been
    #   registered.
    #
    # @abstract Actual listeners passed to {Cql::Future#add_listener} don't
    #   need to extend this class as long as they implement `#success` and
    #   `#failure` methods
    class Listener
      # @param value [Object] actual value the future has been resolved with
      # @return [void]
      def success(value)
      end

      # @param error [Exception] an exception used to fail the future
      # @return [void]
      def failure(error)
      end
    end

    # @private
    module Listeners
      class Success < Listener
        def initialize(block)
          @block = block
        end

        def success(value)
          @block.call(value)
        end

        def failure(error)
          nil
        end
      end

      class Failure < Listener
        def initialize(block)
          @block = block
        end

        def success(value)
          nil
        end

        def failure(error)
          @block.call(error)
        end
      end
    end

    # Returns future value or raises future error
    # @note This method blocks until a future is resolved
    # @raise [Exception] error used to resolve this future if any
    # @return [Object] value used to resolve this future if any
    def get
    end

    # Block until the future has been resolved
    # @note This method won't raise any errors or return anything but the
    #   future itself
    # @return [self]
    def join
    end

    # Run block when promise is fulfilled
    # @note The block can be called synchronously from current thread if the future has already been resolved, or, asynchronously, from background thread upon resolution.
    # @yieldparam value [Object] a value
    # @raise [ArgumentError] if no block given
    # @return [self]
    def on_success(&block)
    end

    # Run block when promise is broken
    # @note The block can be called synchronously from current thread if the future has already been resolved, or, asynchronously, from background thread upon resolution.
    # @yieldparam error [Exception] an error
    # @raise [ArgumentError] if no block given
    # @return [self]
    def on_failure(&block)
    end

    # Add promise listener
    # @note The listener can be notified synchronously, from current thread, if the future has already been resolved, or, asynchronously, from background thread upon resolution.
    # @param listener [Cql::Future::Listener] an object that responds to `#success` and `#failure`
    # @return [self]
    def add_listener(listener)
    end
  end

  # @private
  module Futures
    class Signaled < Future

      def initialize(signal)
        @signal = signal
      end

      def on_success(&block)
        raise ::ArgumentError, "no block given" unless block

        @signal.add_listener(Listeners::Success.new(block))
        self
      end

      def on_failure(&block)
        raise ::ArgumentError, "no block given" unless block

        @signal.add_listener(Listeners::Failure.new(block))
        self
      end

      def add_listener(listener)
        raise ::ArgumentError, "listener must respond to both #success and #failure" unless (listener.respond_to?(:success) && listener.respond_to?(:failure))

        @signal.add_listener(listener)
        self
      end

      def get
        @signal.get
      end

      def join
        @signal.join
        self
      end
    end

    class Broken < Future
      def initialize(error)
        raise ::ArgumentError, "error must be an exception or a string, #{error.inspect} given" unless error.is_a?(::Exception)

        @error = error
      end

      def get
        raise(@error, @error.message, @error.backtrace)
      end

      def on_success
        raise ::ArgumentError, "no block given" unless block_given?
        self
      end

      def on_failure
        raise ::ArgumentError, "no block given" unless block_given?
        yield(@error) rescue nil
        self
      end

      def add_listener(listener)
        raise ::ArgumentError, "listener must respond to both #success and #failure" unless (listener.respond_to?(:success) && listener.respond_to?(:failure))

        listener.failure(@error) rescue nil
        self
      end

      def join
        self
      end
    end

    class Fulfilled < Future
      def initialize(value)
        @value = value
      end

      def get
        @value
      end

      def on_success
        raise ::ArgumentError, "no block given" unless block_given?
        yield(@value) rescue nil
        self
      end

      def on_error
        raise ::ArgumentError, "no block given" unless block_given?
        self
      end

      def add_listener(listener)
        raise ::ArgumentError, "listener must respond to both #success and #failure" unless (listener.respond_to?(:success) && listener.respond_to?(:failure))

        listener.success(@value) rescue nil
        self
      end

      def join
        self
      end
    end
  end

  # @private
  class Promise
    class Signal
      include MonitorMixin

      def initialize
        mon_initialize

        @cond      = new_cond
        @state     = :pending
        @waiting   = 0
        @error     = nil
        @value     = nil
        @listeners = []
      end

      def failure(error)
        raise ::ArgumentError, "error must be an exception, #{error.inspect} given" unless error.is_a?(::Exception)
        return unless @state == :pending

        listeners = nil

        synchronize do
          return unless @state == :pending

          @error = error
          @state = :broken

          listeners, @listeners = @listeners, nil

          @cond.broadcast if @waiting > 0
        end

        listeners.each do |listener|
          listener.failure(error) rescue nil
        end

        self
      end

      def success(value)
        return unless @state == :pending

        listeners = nil

        synchronize do
          return unless @state == :pending

          @value = value
          @state = :fulfilled

          listeners, @listeners = @listeners, nil

          @cond.broadcast if @waiting > 0
        end

        listeners.each do |listener|
          listener.success(value) rescue nil
        end

        self
      end

      def join
        return unless @state == :pending

        synchronize do
          return unless @state == :pending

          @waiting += 1
          @cond.wait while @state == :pending
          @waiting -= 1
        end

        nil
      end

      def get
        join

        raise(@error, @error.message, @error.backtrace) if @state == :broken

        @value
      end

      def add_listener(listener)
        case @state
        when :pending
          synchronize do
            if @state == :pending
              @listeners << listener

              return self
            end
          end

          listener.success(@value) rescue nil if @state == :fulfilled
          listener.failure(@error) rescue nil if @state == :broken
        when :fulfilled
          listener.success(@value) rescue nil
        when :broken
          listener.failure(@error) rescue nil
        end

        self
      end
    end

    attr_reader :future

    def initialize
      @signal = Signal.new
      @future = Futures::Signaled.new(@signal)
    end

    def break(error)
      @signal.failure(error)
      self
    end

    def fulfill(value)
      @signal.success(value)
      self
    end
  end
end
