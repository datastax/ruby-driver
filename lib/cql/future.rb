# encoding: utf-8

require 'thread'


module Cql
  FutureError = Class.new(CqlError)

  # A future represents the value of a process that may not yet have completed.
  #
  # @private
  #
  class Future
    def initialize
      @complete_listeners = []
      @failure_listeners = []
      @state_lock = Mutex.new
    end

    # Combine multiple futures into a new future which completes when all
    # constituent futures complete, or fail when one or more of them fails.
    #
    # The value of the combined future is an array of the values of the
    # constituent futures.
    #
    # @param [Array<Future>] futures the futures to combine
    # @return [Future<Array>] an array of the values of the constituent futures
    #
    def self.combine(*futures)
      if futures.any?
        CombinedFuture.new(*futures)
      else
        completed([])
      end
    end

    # Returns a future which will complete with the value of the first
    # (successful) of the specified futures. If all of the futures fail, the
    # returned future will also fail (with the error of the last failed future).
    #
    # @param [Array<Future>] futures the futures to monitor
    # @return [Future] a future which represents the first completing future
    #
    def self.first(*futures)
      ff = Future.new
      futures.each do |f|
        f.on_complete do |value|
          ff.complete!(value) unless ff.complete?
        end
        f.on_failure do |e|
          ff.fail!(e) if futures.all?(&:failed?)
        end
      end
      ff
    end

    # Creates a new future which is completed.
    #
    # @param [Object, nil] value the value of the created future
    # @return [Future] a completed future
    #
    def self.completed(value=nil)
      CompletedFuture.new(value)
    end

    # Creates a new future which is failed.
    #
    # @param [Error] error the error of the created future
    # @return [Future] a failed future
    #
    def self.failed(error)
      FailedFuture.new(error)
    end

    # Completes the future.
    #
    # This will trigger all completion listeners in the calling thread.
    #
    # @param [Object] v the value of the future
    #
    def complete!(v=nil)
      listeners = nil
      @state_lock.synchronize do
        raise FutureError, 'Future already completed' if complete? || failed?
        @value = v
        listeners = @complete_listeners
        @complete_listeners = nil
      end
      listeners.each do |listener|
        listener.call(v) rescue nil
      end
    end

    # Returns whether or not the future is complete
    #
    def complete?
      defined? @value
    end

    # Registers a listener for when this future completes
    #
    # @yieldparam [Object] value the value of the completed future
    #
    def on_complete(&listener)
      run_immediately = false
      @state_lock.synchronize do
        if complete?
          run_immediately = true
        else
          @complete_listeners << listener
        end
      end
      if run_immediately
        listener.call(value) rescue nil
      end
    end

    # Returns the value of this future, blocking until it is available, if necessary.
    #
    # If the future fails this method will raise the error that failed the future.
    #
    # @return [Object] the value of this future
    #
    def value
      @state_lock.synchronize do
        raise @error if failed?
        return @value if complete?
        t = Thread.current
        u = proc { t.run }
        @complete_listeners << u
        @failure_listeners << u
        @state_lock.sleep
      end
      raise @error if failed?
      @value
    end
    alias_method :get, :value

    # Fails the future.
    #
    # This will trigger all failure listeners in the calling thread.
    #
    # @param [Error] error the error which prevented the value from being determined
    #
    def fail!(error)
      listeners = nil
      @state_lock.synchronize do
        raise FutureError, 'Future already completed' if failed? || complete?
        @error = error
        listeners = @failure_listeners
        @failure_listeners = nil
      end
      listeners.each do |listener|
        listener.call(error) rescue nil
      end
    end

    # Returns whether or not the future is failed.
    #
    def failed?
      !!@error
    end

    # Registers a listener for when this future fails
    #
    # @yieldparam [Error] error the error that failed the future
    #
    def on_failure(&listener)
      run_immediately = false
      @state_lock.synchronize do
        if failed?
          run_immediately = true
        else
          @failure_listeners << listener
        end
      end
      if run_immediately
        listener.call(@error) rescue nil
      end
    end

    # Returns a new future representing a transformation of this future's value.
    #
    # @example
    #   future2 = future1.map { |value| value * 2 }
    #
    # @yieldparam [Object] value the value of this future
    # @yieldreturn [Object] the transformed value
    # @return [Future] a new future representing the transformed value
    #
    def map(&block)
      fp = Future.new
      on_failure { |e| fp.fail!(e) }
      on_complete do |v|
        begin
          vv = block.call(v)
          fp.complete!(vv)
        rescue => e
          fp.fail!(e)
        end
      end
      fp
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
    # @yieldreturn [Future] a future representing the transformed value
    # @return [Future] a new future representing the transformed value
    #
    def flat_map(&block)
      fp = Future.new
      on_failure { |e| fp.fail!(e) }
      on_complete do |v|
        begin
          fpp = block.call(v)
          fpp.on_failure { |e| fp.fail!(e) }
          fpp.on_complete do |vv|
            fp.complete!(vv)
          end
        rescue => e
          fp.fail!(e)
        end
      end
      fp
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
    #   future1.fail!(error)
    #   future2.get # => 'foo'
    #
    # @yieldparam [Object] error the error from the original future
    # @yieldreturn [Object] the value of the new future
    # @return [Future] a new future representing a value recovered from the error
    #
    def recover(&block)
      fp = Future.new
      on_failure do |e|
        begin
          vv = block.call(e)
          fp.complete!(vv)
        rescue => e
          fp.fail!(e)
        end
      end
      on_complete do |v|
        fp.complete!(v)
      end
      fp
    end

    # Returns a new future which represents either the value of the original
    # future, or the value of the future returned by the given block.
    #
    # This is like {#recover} but for cases when the handling of an error is
    # itself asynchronous.
    #
    # If the block raises an error a failed future with that error will be
    # returned (this can be used to transform an error into another error,
    # instead of tranforming an error into a value).
    #
    # @example
    #   future2 = future1.fallback { |error| perform_async_operation }
    #   future1.fail!(error)
    #   future2.get # => whatever the async operation resolved to
    #
    # @yieldparam [Object] error the error from the original future
    # @yieldreturn [Object] the value of the new future
    # @return [Future] a new future representing a value recovered from the error
    #
    def fallback(&block)
      fp = Future.new
      on_failure do |e|
        begin
          fpp = block.call(e)
          fpp.on_failure do |e|
            fp.fail!(e)
          end
          fpp.on_complete do |vv|
            fp.complete!(vv)
          end
        rescue => e
          fp.fail!(e)
        end
      end
      on_complete do |v|
        fp.complete!(v)
      end
      fp
    end
  end

  # @private
  class CompletedFuture < Future
    def initialize(value=nil)
      super()
      complete!(value)
    end
  end

  # @private
  class FailedFuture < Future
    def initialize(error)
      super()
      fail!(error)
    end
  end

  # @private
  class CombinedFuture < Future
    def initialize(*futures)
      super()
      values = [nil] * futures.size
      completed = [false] * futures.size
      futures.each_with_index do |f, i|
        f.on_complete do |v|
          all_done = false
          @state_lock.synchronize do
            values[i] = v
            completed[i] = true
            all_done = completed.all?
          end
          if all_done
            combined_complete!(values)
          end
        end
        f.on_failure do |e|
          unless failed?
            combined_fail!(e)
          end
        end
      end
    end

    alias_method :combined_complete!, :complete!
    private :combined_complete!

    alias_method :combined_fail!, :fail!
    private :combined_fail!

    def complete!(v=nil)
      raise FutureError, 'Cannot complete a combined future'
    end

    def fail!(e)
      raise FutureError, 'Cannot fail a combined future'
    end
  end
end