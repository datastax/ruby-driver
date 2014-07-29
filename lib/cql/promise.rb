# encoding: utf-8

module Cql
  class NPromise
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

      def error!(error)
        synchronize do
          raise ::ArgumentError, "promise has already been fulfilled or broken" unless @state == :pending
          raise ::ArgumentError, "error must be an exception or a string, #{error.inspect} given" unless is_error?(error)

          @error = error
          @state = :broken

          @listeners.each do |listener|
            listener.failure(error)
          end.clear

          @cond.broadcast if @waiting > 0
        end

        self
      end

      def ready!(value)
        synchronize do
          raise ::ArgumentError, "promise has already been fulfilled or broken" unless @state == :pending

          @value = value
          @state = :fulfilled

          @listeners.each do |listener|
            listener.success(value)
          end.clear

          @cond.broadcast if @waiting > 0
        end

        self
      end

      def wait!
        synchronize do
          case @state
          when :pending
            @waiting += 1
            @cond.wait while @state == :pending
            @waiting -= 1

            ::Kernel.raise(@error, @error.message, caller.slice(2..-1)) if @state == :broken

            @value
          when :fulfilled
            @value
          when :broken
            ::Kernel.raise(@error, @error.message, caller.slice(2..-1))
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

      private

      def is_error?(e)
        e.is_a?(::String) || e.is_a?(::Exception) || (e.is_a?(::Class) && e < ::Exception)
      end
    end

    class Future
      def initialize(signal)
        @signal = signal
      end

      def get
        @signal.wait!
      end
    end

    class BrokenFuture
      def initialize(error)
        @error = error
      end

      def get
        raise @error
      end
    end

    attr_reader :future

    def initialize
      @signal = Signal.new
      @future = Future.new(@signal)
    end

    def break(error)
      @signal.error!(error)
      self
    end

    def fulfill(value)
      @signal.ready!(value)
      self
    end
  end
end
