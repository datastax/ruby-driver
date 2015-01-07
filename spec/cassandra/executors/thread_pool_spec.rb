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

require 'spec_helper'

module Cassandra
  module Executors
    describe(ThreadPool) do
      let(:pool_size) { 4 }
      let(:executor)  { ThreadPool.new(pool_size) }

      describe('#execute') do
        it 'executes in the background' do
          complete = false
          lock     = ::Mutex.new

          lock.lock

          executor.execute do
            lock.lock
            complete = true
            lock.unlock
          end

          expect(complete).to be_falsy
          lock.unlock

          wait_for { complete }.to be_truthy
        end

        it 'executes using an available thread' do
          executed = 0
          monitor  = ::Monitor.new
          lock     = ::Mutex.new

          lock.lock

          10.times do |i|
            executor.execute do
              monitor.synchronize do
                executed += 1
              end

              lock.lock
              lock.unlock
            end

            wait_for { executed }.to eq(i + 1) if executed < pool_size
          end

          expect(executed).to eq(pool_size)
          lock.unlock

          wait_for { executed }.to eq(10)
        end
      end

      describe('#shutdown') do
        it 'stops the executor' do
          executed = 0

          executor.shutdown
          Thread.pass

          10.times do
            executor.execute { executed += 1}
            Thread.pass
          end

          expect(executed).to eq(0)
        end
      end
    end
  end
end
