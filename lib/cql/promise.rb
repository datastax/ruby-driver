# encoding: utf-8

module Cql
  module Future
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

    def get
      raise ::NotImplementedError, "must be implemented by a child"
    end

    def on_success(&block)
      raise ::NotImplementedError, "must be implemented by a child"
    end

    def on_failure(&block)
      raise ::NotImplementedError, "must be implemented by a child"
    end

    def add_listener(listener)
      raise ::NotImplementedError, "must be implemented by a child"
    end
  end

  module Futures
    class Signaled
      include Future

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

    class Broken
      include Future

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

    class Fulfilled
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
      end
    end
  end

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
