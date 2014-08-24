require 'spec_helper'

module Cassandra
  class Future
    describe(Value) do
      let(:value)  { double('value') }
      let(:future) { Value.new(value) }

      describe('#get') do
        it 'returns value' do
          expect(future.get).to eq(value)
        end
      end

      describe('#join') do
        it 'returns self' do
          expect(future.join).to eq(future)
        end
      end

      describe('#on_success') do
        it 'raises if no block given' do
          expect { future.on_success }.to raise_error(::ArgumentError, "no block given")
        end

        it 'calls block immediately with value' do
          v = nil
          future.on_success {|value| v = value}
          expect(v).to eq(value)
        end
      end

      describe('#on_failure') do
        it 'raises if no block given' do
          expect { future.on_failure }.to raise_error(::ArgumentError, "no block given")
        end

        it 'ignores block' do
          e = nil
          future.on_failure {|error| e = error}
          expect(e).to be_nil
        end
      end

      describe('#on_complete') do
        it 'raises if no block given' do
          expect { future.on_complete }.to raise_error(::ArgumentError, "no block given")
        end

        it 'calls block with no error and value' do
          e = nil
          v = nil
          future.on_complete {|error, value| e = error; v = value}
          expect(e).to be_nil
          expect(v).to eq(value)
        end
      end

      describe('#add_listener') do
        let(:listener) { double('listener') }

        it 'raises if listener doesn\'t respond to #success and #failure' do
          expect(listener).to receive(:respond_to?).and_return(false)
          expect { future.add_listener(listener) }.to raise_error(::ArgumentError, "listener must respond to both #success and #failure")
        end

        it 'calls listener.success immediately with value' do
          expect(listener).to receive(:respond_to?).with(:success).and_return(true)
          expect(listener).to receive(:respond_to?).with(:failure).and_return(true)
          expect(listener).to receive(:success).once.with(value)
          future.add_listener(listener)
        end

        it 'ignores errors raised by listener' do
          expect(listener).to receive(:respond_to?).with(:success).and_return(true)
          expect(listener).to receive(:respond_to?).with(:failure).and_return(true)
          expect(listener).to receive(:success).once.with(value).and_raise
          future.add_listener(listener)
        end
      end

      describe('#then') do
        it 'raises if no block given' do
          expect { future.then }.to raise_error(ArgumentError, "no block given")
        end

        it 'calls block immediately with value' do
          v = nil
          future.then {|value| v = value}
          expect(v).to eq(value)
        end

        context 'block returns value' do
          it 'returns a new value future' do
            future2 = future.then {|v| 5}
            expect(future2.get).to eq(5)
          end
        end

        context 'block returns a future' do
          it 'returns future' do
            future2 = future.then {|v| future}
            expect(future2).to eq(future)
          end
        end

        context 'block raises error' do
          it 'returns a new error future' do
            future2 = future.then {|v| raise "something bad"}
            expect { future2.get }.to raise_error("something bad")
          end
        end
      end

      describe('#fallback') do
        it 'raises of no block given' do
          expect { future.fallback }.to raise_error(ArgumentError, "no block given")
        end

        it 'returns self' do
          expect(future.fallback {|e| nil}).to eq(future)
        end
      end
    end
  end
end
