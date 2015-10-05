# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
  # A Future represents a result of asynchronous execution. It can be used to
  # block until a value is available or an error has happened, or register a
  # listener to be notified whenever the execution is complete.
  class Future
    # a Future listener to be passed to {Cassandra::Future#add_listener}
    #
    # @note Listener methods can be called from application if a future has
    #   been resolved or failed by the time the listener is registered; or from
    #   background thread if it is resolved/failed after the listener has been
    #   registered.
    #
    # @abstract Actual listeners passed to {Cassandra::Future#add_listener} don't
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
    class Error < Future
      def initialize(error)
        raise ::ArgumentError, "error must be an exception, #{error.inspect} given" unless error.is_a?(::Exception)

        @error = error
      end

      def get(timeout = nil)
        raise(@error, @error.message, @error.backtrace)
      end

      alias :join :get

      def on_success
        raise ::ArgumentError, "no block given" unless block_given?
        self
      end

      def on_failure
        raise ::ArgumentError, "no block given" unless block_given?
        yield(@error) rescue nil
        self
      end

      def on_complete
        raise ::ArgumentError, "no block given" unless block_given?
        yield(nil, @error) rescue nil
        self
      end

      def add_listener(listener)
        unless (listener.respond_to?(:success) && listener.respond_to?(:failure))
          raise ::ArgumentError, "listener must respond to both #success and #failure"
        end

        listener.failure(@error) rescue nil
        self
      end

      def then
        raise ::ArgumentError, "no block given" unless block_given?
        self
      end

      def fallback
        raise ::ArgumentError, "no block given" unless block_given?

        begin
          result = yield(@error)
          result = Value.new(result) unless result.is_a?(Future)
          result
        rescue => e
          Error.new(e)
        end
      end
    end

    # @private
    class Value < Future
      def initialize(value)
        @value = value
      end

      def get(timeout = nil)
        @value
      end

      alias :join :get

      def on_success
        raise ::ArgumentError, "no block given" unless block_given?
        yield(@value) rescue nil
        self
      end

      def on_failure
        raise ::ArgumentError, "no block given" unless block_given?
        self
      end

      def on_complete
        raise ::ArgumentError, "no block given" unless block_given?
        yield(@value, nil) rescue nil
        self
      end

      def add_listener(listener)
        unless (listener.respond_to?(:success) && listener.respond_to?(:failure))
          raise ::ArgumentError, "listener must respond to both #success and #failure"
        end

        listener.success(@value) rescue nil
        self
      end

      def join
        self
      end

      def then
        raise ::ArgumentError, "no block given" unless block_given?

        begin
          result = yield(@value)
          result = Value.new(result) unless result.is_a?(Future)
          result
        rescue => e
          Error.new(e)
        end
      end

      def fallback
        raise ::ArgumentError, "no block given" unless block_given?
        self
      end
    end

    # @private
    class Factory
      def initialize(executor)
        @executor = executor
      end

      def value(value)
        Value.new(value)
      end

      def error(error)
        Error.new(error)
      end

      def promise
        Promise.new(@executor)
      end

      def all(*futures)
        futures   = Array(futures.first) if futures.one?
        monitor   = Monitor.new
        promise   = Promise.new(@executor)
        remaining = futures.length
        values    = Array.new(remaining)

        futures.each_with_index do |future, i|
          future.on_complete do |v, e|
            if e
              promise.break(e)
            else
              done = false
              monitor.synchronize do
                remaining -= 1
                done = (remaining == 0)
                values[i] = v
              end
              promise.fulfill(values) if done
            end
          end
        end
        promise.future
      end
    end

    # @private
    @@factory = Factory.new(Executors::SameThread.new)

    # Returns a future resolved to a given value
    # @param value [Object] value for the future
    # @return [Cassandra::Future<Object>] a future value
    def self.value(value)
      @@factory.value(value)
    end

    # Returns a future resolved to a given error
    # @param error [Exception] error for the future
    # @return [Cassandra::Future<Exception>] a future error
    def self.error(error)
      @@factory.error(error)
    end

    # Returns a future that resolves with values of all futures
    # @overload all(*futures)
    #   @param *futures [Cassandra::Future] futures to combine
    #   @return [Cassandra::Future<Array<Object>>] a combined future
    # @overload all(futures)
    #   @param futures [Enumerable<Cassandra::Future>] list of futures to
    #     combine
    #   @return [Cassandra::Future<Array<Object>>] a combined future
    def self.all(*futures)
      @@factory.all(*futures)
    end

    # Returns a new promise instance
    def self.promise
      @@factory.promise
    end

    # @private
    def initialize(signal)
      @signal = signal
    end

    # Run block when future resolves to a value
    # @note The block can be called synchronously from current thread if the
    #   future has already been resolved, or, asynchronously, from background
    #   thread upon resolution.
    # @yieldparam value [Object] a value
    # @raise [ArgumentError] if no block given
    # @return [self]
    def on_success(&block)
      raise ::ArgumentError, "no block given" unless block_given?
      @signal.on_success(&block)
      self
    end

    # Run block when future resolves to error
    # @note The block can be called synchronously from current thread if the
    #   future has already been resolved, or, asynchronously, from background
    #   thread upon resolution.
    # @yieldparam error [Exception] an error
    # @raise [ArgumentError] if no block given
    # @return [self]
    def on_failure(&block)
      raise ::ArgumentError, "no block given" unless block_given?
      @signal.on_failure(&block)
      self
    end

    # Run block when future resolves. The block will always be called with 2
    #   arguments - value and error. In case a future resolves to an error, the
    #   error argument will be non-nil.
    # @note The block can be called synchronously from current thread if the
    #   future has already been resolved, or, asynchronously, from background
    #   thread upon resolution.
    # @yieldparam value [Object, nil] a value or nil
    # @yieldparam error [Exception, nil] an error or nil
    # @raise [ArgumentError] if no block given
    # @return [self]
    def on_complete(&block)
      raise ::ArgumentError, "no block given" unless block_given?
      @signal.on_complete(&block)
      self
    end

    # Add future listener
    # @note The listener can be notified synchronously, from current thread, if
    #   the future has already been resolved, or, asynchronously, from
    #   background thread upon resolution.
    # @note that provided listener doesn't have to extend
    #   {Cassandra::Future::Listener}, only conform to the same interface
    # @param listener [Cassandra::Future::Listener] an object that responds to
    #   `#success` and `#failure`
    # @return [self]
    def add_listener(listener)
      unless (listener.respond_to?(:success) && listener.respond_to?(:failure))
        raise ::ArgumentError, "listener must respond to both #success and #failure"
      end

      @signal.add_listener(listener)
      self
    end

    # Returns a new future that will resolve to the result of the block.
    # Besides regular values, block can return other futures, which will be
    # transparently unwrapped before resolving the future from this method.
    #
    # @example Block returns a value
    #   future_users = session.execute_async('SELECT * FROM users WHERE user_name = ?', 'Sam')
    #   future_user  = future_users.then {|users| users.first}
    #
    # @example Block returns a future
    #   future_statement = session.prepare_async('SELECT * FROM users WHERE user_name = ?')
    #   future_users     = future_statement.then {|statement| session.execute_async(statement, 'Sam')}
    #
    # @note The block can be called synchronously from current thread if the
    #   future has already been resolved, or, asynchronously, from background
    #   thread upon resolution.
    # @yieldparam value [Object] a value
    # @yieldreturn [Cassandra::Future, Object] a future or a value to be
    #   wrapped in a future
    # @raise [ArgumentError] if no block given
    # @return [Cassandra::Future] a new future
    def then(&block)
      raise ::ArgumentError, "no block given" unless block_given?
      @signal.then(&block)
    end

    # Returns a new future that will resolve to the result of the block in case
    # of an error. Besides regular values, block can return other futures,
    # which will be transparently unwrapped before resolving the future from
    # this method.
    #
    # @example Recovering from errors
    #   future_error = session.execute_async('SELECT * FROM invalid-table')
    #   future       = future_error.fallback {|error| "Execution failed with #{error.class.name}: #{error.message}"}
    #
    # @example Executing something else on error
    #   future_error = session.execute_async('SELECT * FROM invalid-table')
    #   future       = future_error.fallback {|e| session.execute_async('SELECT * FROM another-table')}
    #
    # @note The block can be called synchronously from current thread if the
    #   future has already been resolved, or, asynchronously, from background
    #   thread upon resolution.
    # @yieldparam error [Exception] an error
    # @yieldreturn [Cassandra::Future, Object] a future or a value to be
    #   wrapped in a future
    # @raise [ArgumentError] if no block given
    # @return [Cassandra::Future] a new future
    def fallback(&block)
      raise ::ArgumentError, "no block given" unless block_given?
      @signal.fallback(&block)
    end

    # Returns future value or raises future error
    #
    # @note This method blocks until a future is resolved or a times out
    #
    # @param timeout [nil, Numeric] a maximum number of seconds to block
    #   current thread for while waiting for this future to resolve. Will
    #   wait indefinitely if passed `nil`.
    #
    # @raise [Errors::TimeoutError] raised when wait time exceeds the timeout
    # @raise [Exception] raises when the future has been resolved with an
    #   error. The original exception will be raised.
    #
    # @return [Object] the value that the future has been resolved with
    def get(timeout = nil)
      @signal.get(timeout)
    end

    alias :join :get
  end

  # @private
  class Promise
    # @private
    class Signal
      # @private
      module Listeners
        class Success < Future::Listener
          def initialize(&block)
            @block = block
          end

          def success(value)
            @block.call(value)
          end

          def failure(error)
            nil
          end
        end

        class Failure < Future::Listener
          def initialize(&block)
            @block = block
          end

          def success(value)
            nil
          end

          def failure(error)
            @block.call(error)
          end
        end

        class Complete < Future::Listener
          def initialize(&block)
            @block = block
          end

          def success(value)
            @block.call(value, nil)
          end

          def failure(error)
            @block.call(nil, error)
          end
        end

        class Then < Future::Listener
          def initialize(promise, &block)
            @promise = promise
            @block   = block
          end

          def success(value)
            result = @block.call(value)

            if result.is_a?(Future)
              @promise.observe(result)
            else
              @promise.fulfill(result)
            end
          rescue => e
            @promise.break(e)
          ensure
            @promise = @block = nil
          end

          def failure(error)
            @promise.break(error)
          ensure
            @promise = @block = nil
          end
        end

        class Fallback < Future::Listener
          def initialize(promise, &block)
            @promise = promise
            @block   = block
          end

          def success(value)
            @promise.fulfill(value)
          ensure
            @promise = @block = nil
          end

          def failure(error)
            result = @block.call(error)

            if result.is_a?(Future)
              @promise.observe(result)
            else
              @promise.fulfill(result)
            end
          rescue => e
            @promise.break(e)
          ensure
            @promise = @block = nil
          end
        end
      end

      include MonitorMixin

      def initialize(executor)
        mon_initialize

        @cond      = new_cond
        @executor  = executor
        @state     = :pending
        @waiting   = 0
        @error     = nil
        @value     = nil
        @listeners = []
      end

      def failure(error)
        unless error.is_a?(::Exception)
          raise ::ArgumentError, "error must be an exception, #{error.inspect} given"
        end

        return unless @state == :pending

        listeners = nil

        synchronize do
          return unless @state == :pending

          @error = error
          @state = :broken

          listeners, @listeners = @listeners, nil
        end

        @executor.execute do
          listeners.each do |listener|
            listener.failure(error) rescue nil
          end

          synchronize do
            @cond.broadcast if @waiting > 0
          end
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
        end

        @executor.execute do
          listeners.each do |listener|
            listener.success(value) rescue nil
          end

          synchronize do
            @cond.broadcast if @waiting > 0
          end
        end

        self
      end

      # @param timeout [nil, Numeric] a maximum number of seconds to block
      #   current thread for while waiting for this future to resolve. Will
      #   wait indefinitely if passed `nil`.
      #
      # @raise [ArgumentError] raised when a negative timeout is given
      # @raise [Errors::TimeoutError] raised when wait time exceeds the timeout
      # @raise [Exception] raises when the future has been resolved with an
      #   error. The original exception will be raised.
      #
      # @return [Object] the value that the future has been resolved with
      def get(timeout = nil)
        timeout = timeout && Float(timeout)

        if timeout
          raise ::ArgumentError, "timeout cannot be negative, #{timeout.inspect} given" if timeout < 0

          now      = ::Time.now
          deadline = now + timeout
        end

        if @state == :pending
          synchronize do
            if @state == :pending
              @waiting += 1
              while @state == :pending
                if deadline
                  @cond.wait(deadline - now)
                  now = ::Time.now
                  break if now >= deadline
                else
                  @cond.wait
                end
              end
              @waiting -= 1
            end
          end

          if @state == :pending
            total_wait = deadline - now
            raise Errors::TimeoutError, "Future did not complete within #{timeout.inspect} seconds. Wait time: #{total_wait.inspect}"
          end
        end

        if @state == :broken
          raise(@error, @error.message, @error.backtrace)
        end

        @value
      end

      alias :join :get

      def add_listener(listener)
        if @state == :pending
          synchronize do
            if @state == :pending
              @listeners << listener

              return self
            end
          end
        end

        listener.success(@value) rescue nil if @state == :fulfilled
        listener.failure(@error) rescue nil if @state == :broken

        self
      end

      def on_success(&block)
        if @state == :pending
          synchronize do
            if @state == :pending
              @listeners << Listeners::Success.new(&block)
              return self
            end
          end
        end

        yield(@value) rescue nil if @state == :fulfilled

        self
      end

      def on_failure(&block)
        if @state == :pending
          synchronize do
            if @state == :pending
              @listeners << Listeners::Failure.new(&block)
              return self
            end
          end
        end

        yield(@error) rescue nil if @state == :broken

        self
      end

      def on_complete(&block)
        if @state == :pending
          synchronize do
            if @state == :pending
              @listeners << Listeners::Complete.new(&block)
              return self
            end
          end
        end

        yield(@value, @error) rescue nil

        self
      end

      def then(&block)
        if @state == :pending
          synchronize do
            if @state == :pending
              promise  = Promise.new(@executor)
              listener = Listeners::Then.new(promise, &block)
              @listeners << listener
              return promise.future
            end
          end
        end

        return Future::Error.new(@error) if @state == :broken

        begin
          result = yield(@value)
          result = Future::Value.new(result) unless result.is_a?(Future)
          result
        rescue => e
          Future::Error.new(e)
        end
      end

      def fallback(&block)
        if @state == :pending
          synchronize do
            if @state == :pending
              promise  = Promise.new(@executor)
              listener = Listeners::Fallback.new(promise, &block)
              @listeners << listener
              return promise.future
            end
          end
        end

        return Future::Value.new(@value) if @state == :fulfilled

        begin
          result = yield(@error)
          result = Future::Value.new(result) unless result.is_a?(Future)
          result
        rescue => e
          Future::Error.new(e)
        end
      end
    end

    attr_reader :future

    def initialize(executor)
      @signal = Signal.new(executor)
      @future = Future.new(@signal)
    end

    def break(error)
      @signal.failure(error)
      self
    end

    def fulfill(value)
      @signal.success(value)
      self
    end

    def observe(future)
      future.add_listener(@signal)
    end
  end
end
