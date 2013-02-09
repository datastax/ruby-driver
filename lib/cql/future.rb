# encoding: utf-8

module Cql
  FutureError = Class.new(CqlError)

  class Future
    def initialize
      @complete_listeners = []
      @failure_listeners = []
      @value_barrier = Queue.new
      @state_lock = Mutex.new
    end

    def self.combine(*futures)
      CombinedFuture.new(*futures)
    end

    def self.completed(value=nil)
      CompletedFuture.new(value)
    end

    def self.failed(error)
      FailedFuture.new(error)
    end

    def complete!(v=nil)
      @state_lock.synchronize do
        raise FutureError, 'Future already completed' if complete? || failed?
        @value = v
        @complete_listeners.each do |listener|
          listener.call(@value)
        end
      end
    ensure
      @state_lock.synchronize do
        @value_barrier << :ping
      end
    end

    def complete?
      defined? @value
    end

    def on_complete(&listener)
      @state_lock.synchronize do
        if complete?
          listener.call(value)
        else
          @complete_listeners << listener
        end
      end
    end

    def value
      raise @error if @error
      return @value if defined? @value
      @value_barrier.pop
      raise @error if @error
      return @value
    end
    alias_method :get, :value

    def fail!(error)
      @state_lock.synchronize do
        raise FutureError, 'Future already completed' if failed? || complete?
        @error = error
        @failure_listeners.each do |listener|
          listener.call(error)
        end
      end
    ensure
      @state_lock.synchronize do
        @value_barrier << :ping
      end
    end

    def failed?
      !!@error
    end

    def on_failure(&listener)
      @state_lock.synchronize do
        if failed?
          listener.call(@error)
        else
          @failure_listeners << listener
        end
      end
    end

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
  end

  class CompletedFuture < Future
    def initialize(value=nil)
      super()
      complete!(value)
    end
  end

  class FailedFuture < Future
    def initialize(error)
      super()
      fail!(error)
    end
  end

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