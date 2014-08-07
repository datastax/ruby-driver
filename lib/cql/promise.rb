# encoding: utf-8

module Cql
  # A Future represents a result of asynchronous execution. It can be used to
  # block until a value is available or an error has happened, or register a
  # listener to be notified whenever the execution is complete.
  class Future
    # @private
    module Listeners
      class Success
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

      class Failure
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

    # Wait for value or error
    # @note This method blocks until a future is ready
    # @raise [Exception] error
    # @return [Object] value
    def get
    end

    # Run block when promise is fulfilled
    # @note The block can be called synchronously from current thread if the future has already been resolved, or, asynchronously, from background thread upon resolution.
    # @yieldparam [Object] value
    # @return [self]
    def on_success(&block)
    end

    # Run block when promise is broken
    # @note The block can be called synchronously from current thread if the future has already been resolved, or, asynchronously, from background thread upon resolution.
    # @yieldparam [Exception] error
    # @return [self]
    def on_failure(&block)
    end

    # Add promise listener
    # @note The listener can be notified synchronously, from current thread, if the future has already been resolved, or, asynchronously, from background thread upon resolution.
    # @param listener [#success, #failure] an object that responds to `#success` and `#failure`
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
        @signal.add_listener(Listeners::Success.new(block))
        self
      end

      def on_failure(&block)
        @signal.add_listener(Listeners::Failure.new(block))
        self
      end

      def add_listener(listener)
        @signal.add_listener(listener)
        self
      end

      def get
        @signal.wait
      end
    end

    class Broken < Future
      def initialize(error)
        raise ::ArgumentError, "error must be an exception or a string, #{error.inspect} given" unless error.is_a?(::Exception)

        @error = error
      end

      def get
        raise(@error, @error.message, caller.slice(2..-1))
      end

      def on_success
        self
      end

      def on_failure
        yield(@error)
        self
      end

      def add_listener(listener)
        listener.failure(@error)
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
        yield(@value)
        self
      end

      def on_error
        self
      end

      def add_listener(listener)
        listener.success(@value)
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

        listeners = nil

        synchronize do
          raise ::ArgumentError, "promise has already been fulfilled or broken" unless @state == :pending

          @error = error
          @state = :broken

          listeners, @listeners = @listeners, nil

          @cond.broadcast if @waiting > 0
        end

        listeners.each do |listener|
          listener.failure(error)
        end

        self
      end

      def success(value)
        listeners = nil

        synchronize do
          raise ::ArgumentError, "promise has already been fulfilled or broken" unless @state == :pending

          @value = value
          @state = :fulfilled

          listeners, @listeners = @listeners, nil

          @cond.broadcast if @waiting > 0
        end

        listeners.each do |listener|
          listener.success(value)
        end

        self
      end

      def wait
        synchronize do
          case @state
          when :pending
            @waiting += 1
            @cond.wait while @state == :pending
            @waiting -= 1

            raise(@error, @error.message, caller.slice(2..-1)) if @state == :broken

            @value
          when :fulfilled
            @value
          when :broken
            raise(@error, @error.message, caller.slice(2..-1))
          end
        end
      end

      def add_listener(listener)
        raise ::ArgumentError, "listener must respond to both #success and #failure" unless (listener.respond_to?(:success) && listener.respond_to?(:failure))

        synchronize do
          case @state
          when :pending
            @listeners << listener
          when :fulfilled
            listener.success(@value)
          when :broken
            listener.failure(@error)
          end
        end

        self
      end
    end

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

    def future
      @future
    end
  end
end
