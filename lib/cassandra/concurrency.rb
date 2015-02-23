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
  # @private
  module Concurrency
    # reimplementation of stdlib ConditionVariable that returns the number of
    # seconds spent in Mutex#sleep
    class ConditionVariable
      def initialize
        @threads = ::Array.new
        @lock    = ::Mutex.new
      end

      # Releases the lock held in +mutex+ and waits; reacquires the lock on
      # wakeup.
      #
      # If `timeout` is given, this method returns after `timeout` seconds
      # passed, even if no other thread doesn't signal.
      #
      # @param mutex   [Mutex] the mutex
      # @param timeout [Numeric, nil] maximum number of seconds this thread
      #   will be blocked for
      #
      # @return [Numeric] number of seconds spent in sleep
      def wait(mutex, timeout = nil)
        @lock.synchronize do
          @threads.push(::Thread.current)
        end

        mutex.sleep(timeout)
      ensure
        @lock.synchronize do
          @threads.delete(::Thread.current)
        end
      end

      # Wakes up the first thread in line waiting for this lock.
      #
      # @return [Cassandra::Concurrency::ConditionVariable] self
      def signal
        begin
          t = @lock.synchronize { @threads.shift }
          t && t.run
        rescue ::ThreadError
          retry
        end

        self
      end

      # Wakes up all threads waiting for this lock.
      #
      # @return [Cassandra::Concurrency::ConditionVariable] self
      def broadcast
        threads = nil

        @lock.synchronize do
          threads  = @threads
          @threads = ::Array.new
        end

        threads.each do |t|
          begin
            t.run
          rescue ::ThreadError
          end
        end.clear

        self
      end
    end
  end
end
