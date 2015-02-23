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

require 'spec_helper'

module Cassandra
  class Promise
    class Signal
      module Listeners
        describe(Complete) do
          describe('#success') do
            it 'calls block with value' do
              value    = nil
              error    = nil
              listener = Complete.new do |v, e|
                value = v
                error = e
              end

              listener.success(123)
              expect(value).to eq(123)
            end
          end
          describe('#failure') do
            it 'calls block with error' do
              value    = nil
              error    = nil
              listener = Complete.new do |v, e|
                value = v
                error = e
              end

              listener.failure(123)
              expect(error).to eq(123)
            end
          end
        end
      end
    end
  end

  describe(Future) do
    let(:signal) { double('promise signal') }
    let(:future) { Future.new(signal) }

    describe('.all') do
      it 'succeeds when all futures succeed' do
        futures = 10.times.map {|i| Future.value(i)}
        expect(Future.all(futures).get).to eq([0,1,2,3,4,5,6,7,8,9])
      end

      it 'fails when any futures fail' do
        futures = 9.times.map {|i| Future.value(i)}
        futures << Future.error(RuntimeError.new("something happened"))
        expect { Future.all(futures).get }.to raise_error("something happened")
      end
    end

    describe('#get') do
      it 'delegates to signal' do
        expect(signal).to receive(:get).once
        future.get
      end
    end

    describe('#join') do
      it 'delegates to signal' do
        expect(signal).to receive(:get).once
        future.join
      end
    end

    describe('#on_success') do
      it 'raises if no block given' do
        expect { future.on_success }.to raise_error(::ArgumentError, "no block given")
      end

      it 'delegates to signal' do
        expect(signal).to receive(:on_success).once
        future.on_success {|value| nil}
      end
    end

    describe('#on_failure') do
      it 'raises if no block given' do
        expect { future.on_failure }.to raise_error(::ArgumentError, "no block given")
      end

      it 'delegates to signal' do
        expect(signal).to receive(:on_failure).once
        future.on_failure {|error| nil}
      end
    end

    describe('#on_complete') do
      it 'raises if no block given' do
        expect { future.on_complete }.to raise_error(::ArgumentError, "no block given")
      end

      it 'delegates to signal' do
        expect(signal).to receive(:on_complete).once
        future.on_complete {|error, value| nil}
      end
    end

    describe('#add_listener') do
      let(:listener) { double('listener') }

      it 'raises if listener doesn\'t respond to #success and #failure' do
        expect(listener).to receive(:respond_to?).and_return(false)
        expect { future.add_listener(listener) }.to raise_error(::ArgumentError, "listener must respond to both #success and #failure")
      end

      it 'delegates to signal' do
        expect(listener).to receive(:respond_to?).with(:success).and_return(true)
        expect(listener).to receive(:respond_to?).with(:failure).and_return(true)
        expect(signal).to receive(:add_listener).once.with(listener)
        expect(future.add_listener(listener)).to eq(future)
      end
    end

    describe('#then') do
      it 'raises if no block given' do
        expect { future.then }.to raise_error(ArgumentError, "no block given")
      end

      it 'delegates to signal' do
        expect(signal).to receive(:then).once
        future.then {|v| nil}
      end
    end

    describe('#fallback') do
      it 'raises of no block given' do
        expect { future.fallback }.to raise_error(ArgumentError, "no block given")
      end

      it 'delegates to signal' do
        expect(signal).to receive(:fallback).once
        future.fallback {|e| nil}
      end
    end
  end
end
