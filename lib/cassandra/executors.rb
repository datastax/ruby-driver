# encoding: utf-8

#--
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
#++

module Cassandra
  # @private
  module Executors
    class ThreadPool
      class Task
        def initialize(promise, *args, &block)
          @promise = promise
          @args    = args
          @block   = block
        end

        def run
          result = @block.call(*@args)

          if result.is_a?(Future)
            @promise.observe(result)
          else
            @promise.fulfill(result)
          end
        rescue ::Exception => e
          @promise.break(e)
        ensure
          @promise = @args = @block = nil
        end
      end

      include MonitorMixin

      def initialize(size)
        mon_initialize

        @cond    = new_cond
        @tasks   = ::Array.new
        @waiting = 0
        @pool    = ::Array.new(size, &method(:spawn_thread))
      end

      def execute(*args, &block)
        promise = Cassandra::Future.promise

        synchronize do
          @tasks << Task.new(promise, *args, &block)
          @cond.signal if @waiting > 0
        end

        promise.future
      end

      private

      def spawn_thread(i)
        Thread.new(&method(:run))
      end

      def run
        loop do
          tasks = nil

          synchronize do
            @waiting += 1
            @cond.wait while @tasks.empty?
            @waiting -= 1

            tasks  = @tasks
            @tasks = ::Array.new
          end

          tasks.each(&:run).clear
        end
      end
    end

    class SameThread
      def execute(*args, &block)
        result = yield(*args)
        if result.is_a?(Future)
          return result
        else
          Future.value(result)
        end
      rescue ::Exception => e
        Future.error(e)
      end
    end
  end
end
