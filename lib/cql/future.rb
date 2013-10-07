# encoding: utf-8

require 'thread'


module Cql
  FutureError = Class.new(CqlError)

  # A promise of delivering a value some time in the future.
  #
  # A promise is the write end of a Promise/Future pair. It can be fulfilled
  # with a value or failed with an error. The value can be read through the
  # future returned by {#future}.
  #
  # @private
  class Promise
    attr_reader :future

    def initialize
      @future = CompletableFuture.new
    end

    # Fulfills the promise.
    #
    # This will resolve this promise's future, and trigger all listeners of that
    # future. The value of the future will be the specified value, or nil if
    # no value is specified.
    #
    # @param [Object] value the value of the future
    def fulfill(value=nil)
      @future.resolve(value)
    end

    # Fails the promise.
    #
    # This will fail this promise's future, and trigger all listeners of that
    # future.
    #
    # @param [Error] error the error which prevented the promise to be fulfilled
    def fail(error)
      @future.fail(error)
    end

    # Observe a future and fulfill the promise with the future's value when the
    # future resolves, or fail with the future's error when the future fails.
    #
    # @param [Cql::Future] future the future to observe
    def observe(future)
      future.on_value { |v| fulfill(v) }
      future.on_failure { |e| fail(e) }
    end

    # Run the given block and fulfill this promise with its result. If the block
    # raises an error, fail this promise with the error.
    #
    # All arguments given will be passed onto the block.
    #
    # @example
    #   promise.try { 3 + 4 }
    #   promise.future.value # => 7
    #
    # @example
    #   promise.try do
    #     do_something_that_will_raise_an_error
    #   end
    #   promise.future.value # => (raises error)
    #
    # @example
    #   promise.try('foo', 'bar', &proc_taking_two_arguments)
    #
    # @yieldparam [Array] ctx the arguments passed to {#try}
    def try(*ctx)
      fulfill(yield(*ctx))
    rescue => e
      fail(e)
    end
  end

  # @private
  module FutureFactories
    # Combines multiple futures into a new future which resolves when all
    # constituent futures complete, or fails when one or more of them fails.
    #
    # The value of the combined future is an array of the values of the
    # constituent futures.
    #
    # @param [Array<Cql::Future>] futures the futures to combine
    # @return [Cql::Future<Array>] an array of the values of the constituent
    #   futures
    def all(*futures)
      if futures.any?
        CombinedFuture.new(futures)
      else
        resolved([])
      end
    end

    # Returns a future which will be resolved with the value of the first
    # (resolved) of the specified futures. If all of the futures fail, the
    # returned future will also fail (with the error of the last failed future).
    #
    # @param [Array<Cql::Future>] futures the futures to monitor
    # @return [Cql::Future] a future which represents the first completing future
    def first(*futures)
      if futures.any?
        FirstFuture.new(futures)
      else
        resolved
      end
    end

    # Creates a new pre-resolved future.
    #
    # @param [Object, nil] value the value of the created future
    # @return [Cql::Future] a resolved future
    def resolved(value=nil)
      ResolvedFuture.new(value)
    end

    # Creates a new pre-failed future.
    #
    # @param [Error] error the error of the created future
    # @return [Cql::Future] a failed future
    def failed(error)
      FailedFuture.new(error)
    end
  end

  # @private
  module FutureCombinators
    # Returns a new future representing a transformation of this future's value.
    #
    # @example
    #   future2 = future1.map { |value| value * 2 }
    #
    # @yieldparam [Object] value the value of this future
    # @yieldreturn [Object] the transformed value
    # @return [Cql::Future] a new future representing the transformed value
    def map(&block)
      p = Promise.new
      on_failure { |e| p.fail(e) }
      on_value do |v|
        p.try(v, &block)
      end
      p.future
    end

    # Returns a new future representing a transformation of this future's value,
    # but where the transformation itself may be asynchronous.
    #
    # @example
    #   future2 = future1.flat_map { |value| method_returning_a_future(value) }
    #
    # This method is useful when you want to chain asynchronous operations.
    #
    # @yieldparam [Object] value the value of this future
    # @yieldreturn [Cql::Future] a future representing the transformed value
    # @return [Cql::Future] a new future representing the transformed value
    def flat_map(&block)
      p = Promise.new
      on_failure { |e| p.fail(e) }
      on_value do |v|
        begin
          f = block.call(v)
          p.observe(f)
        rescue => e
          p.fail(e)
        end
      end
      p.future
    end

    # Returns a new future which represents either the value of the original
    # future, or the result of the given block, if the original future fails.
    #
    # This method is similar to{#map}, but is triggered by a failure. You can
    # also think of it as a `rescue` block for asynchronous operations.
    #
    # If the block raises an error a failed future with that error will be
    # returned (this can be used to transform an error into another error,
    # instead of tranforming an error into a value).
    #
    # @example
    #   future2 = future1.recover { |error| 'foo' }
    #   future1.fail(error)
    #   future2.value # => 'foo'
    #
    # @yieldparam [Object] error the error from the original future
    # @yieldreturn [Object] the value of the new future
    # @return [Cql::Future] a new future representing a value recovered from the error
    def recover(&block)
      p = Promise.new
      on_failure do |e|
        p.try(e, &block)
      end
      on_value do |v|
        p.fulfill(v)
      end
      p.future
    end

    # Returns a new future which represents either the value of the original
    # future, or the value of the future returned by the given block.
    #
    # This is like {#recover} but for cases when the handling of an error is
    # itself asynchronous. In other words, {#fallback} is to {#recover} what
    # {#flat_map} is to {#map}.
    #
    # If the block raises an error a failed future with that error will be
    # returned (this can be used to transform an error into another error,
    # instead of tranforming an error into a value).
    #
    # @example
    #   result = http_get('/foo/bar').fallback do |error|
    #     http_get('/baz')
    #   end
    #   result.value # either the response to /foo/bar, or if that failed
    #                # the response to /baz
    #
    # @yieldparam [Object] error the error from the original future
    # @yieldreturn [Object] the value of the new future
    # @return [Cql::Future] a new future representing a value recovered from the
    #   error
    def fallback(&block)
      p = Promise.new
      on_failure do |e|
        begin
          f = block.call(e)
          p.observe(f)
        rescue => e
          p.fail(e)
        end
      end
      on_value do |v|
        p.fulfill(v)
      end
      p.future
    end
  end

  # @private
  module FutureCallbacks
    # Registers a listener that will be called when this future completes,
    # i.e. resolves or fails. The listener will be called with the future as
    # solve argument
    #
    # @yieldparam [Cql::Future] future the future
    def on_complete(&listener)
      run_immediately = false
      @lock.synchronize do
        if @resolved || @failed
          run_immediately = true
        else
          @complete_listeners << listener
        end
      end
      if run_immediately
        listener.call(self) rescue nil
      end
      nil
    end

    # Registers a listener that will be called when this future becomes
    # resolved. The listener will be called with the value of the future as
    # sole argument.
    #
    # @yieldparam [Object] value the value of the resolved future
    def on_value(&listener)
      run_immediately = false
      @lock.synchronize do
        if @resolved
          run_immediately = true
        elsif !@failed
          @value_listeners << listener
        end
      end
      if run_immediately
        listener.call(value) rescue nil
      end
      nil
    end

    # Registers a listener that will be called when this future fails. The
    # lisener will be called with the error that failed the future as sole
    # argument.
    #
    # @yieldparam [Error] error the error that failed the future
    def on_failure(&listener)
      run_immediately = false
      @lock.synchronize do
        if @failed
          run_immediately = true
        elsif !@resolved
          @failure_listeners << listener
        end
      end
      if run_immediately
        listener.call(@error) rescue nil
      end
      nil
    end
  end

  # A future represents the value of a process that may not yet have completed.
  #
  # @see Cql::Promise
  # @private
  class Future
    extend FutureFactories
    include FutureCombinators
    include FutureCallbacks

    def initialize
      @lock = Mutex.new
      @resolved = false
      @failed = false
      @failure_listeners = []
      @value_listeners = []
      @complete_listeners = []
    end

    # Returns the value of this future, blocking until it is available if
    # necessary.
    #
    # If the future fails this method will raise the error that failed the
    # future.
    #
    # @return [Object] the value of this future
    def value
      @lock.synchronize do
        raise @error if @failed
        return @value if @resolved
        t = Thread.current
        u = proc { t.wakeup }
        @value_listeners << u
        @failure_listeners << u
        while true
          raise @error if @failed
          return @value if @resolved
          @lock.sleep(1)
        end
      end
    end

    # Returns true if this future is resolved or failed
    def completed?
      resolved? || failed?
    end

    # Returns true if this future is resolved
    def resolved?
      @lock.synchronize { @resolved }
    end

    # Returns true if this future has failed
    def failed?
      @lock.synchronize { @failed }
    end
  end

  # @private
  class CompletableFuture < Future
    def resolve(v=nil)
      value_listeners = nil
      complete_listeners = nil
      @lock.synchronize do
        raise FutureError, 'Future already completed' if @resolved || @failed
        @resolved = true
        @value = v
        value_listeners = @value_listeners
        complete_listeners = @complete_listeners
        @value_listeners = nil
        @failure_listeners = nil
        @complete_listeners = nil
      end
      value_listeners.each do |listener|
        listener.call(v) rescue nil
      end
      complete_listeners.each do |listener|
        listener.call(self) rescue nil
      end
      nil
    end

    def fail(error)
      failure_listeners = nil
      complete_listeners = nil
      @lock.synchronize do
        raise FutureError, 'Future already completed' if @failed || @resolved
        @failed = true
        @error = error
        failure_listeners = @failure_listeners
        complete_listeners = @complete_listeners
        @value_listeners = nil
        @failure_listeners = nil
        @complete_listeners = nil
      end
      failure_listeners.each do |listener|
        listener.call(error) rescue nil
      end
      complete_listeners.each do |listener|
        listener.call(self) rescue nil
      end
      nil
    end
  end

  # @private
  class CombinedFuture < CompletableFuture
    def initialize(futures)
      super()
      values = Array.new(futures.size)
      remaining = futures.size
      futures.each_with_index do |f, i|
        f.on_value do |v|
          @lock.synchronize do
            values[i] = v
            remaining -= 1
          end
          if remaining == 0
            resolve(values)
          end
        end
        f.on_failure do |e|
          unless failed?
            fail(e)
          end
        end
      end
    end
  end

  # @private
  class FirstFuture < CompletableFuture
    def initialize(futures)
      super()
      futures.each do |f|
        f.on_value do |value|
          resolve(value) unless completed?
        end
        f.on_failure do |e|
          fail(e) if futures.all?(&:failed?)
        end
      end
    end
  end

  # @private
  class ResolvedFuture < Future
    def initialize(value=nil)
      super()
      @value = value
      @resolved = true
    end
  end

  # @private
  class FailedFuture < Future
    def initialize(error)
      super()
      @error = error
      @failed = true
    end
  end
end