require 'spec_helper'

module Cassandra
  class Future
    describe(Error) do
      describe('.new') do
        it 'raises if error is not an Exception' do
          error = double('error')
          expect { Error.new(error) }.to raise_error(ArgumentError, "error must be an exception or a string, #{error.inspect} given")
        end
      end

      let(:error)  { RuntimeError.new('error') }
      let(:future) { Error.new(error) }

      describe('#get') do
        it 'raises error' do
          expect { future.get }.to raise_error(error.message)
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

        it 'ignores block' do
          v = nil
          future.on_success {|value| v = value}
          expect(v).to be_nil
        end
      end

      describe('#on_failure') do
        it 'raises if no block given' do
          expect { future.on_failure }.to raise_error(::ArgumentError, "no block given")
        end

        it 'calls block immediately with error' do
          e = nil
          future.on_failure {|error| e = error}
          expect(e).to eq(error)
        end
      end

      describe('#on_complete') do
        it 'raises if no block given' do
          expect { future.on_complete }.to raise_error(::ArgumentError, "no block given")
        end

        it 'calls block with error and no value' do
          e = nil
          v = nil
          future.on_complete {|error, value| e = error; v = value}
          expect(e).to eq(error)
          expect(v).to be_nil
        end
      end

      describe('#add_listener') do
        let(:listener) { double('listener') }

        it 'raises if listener doesn\'t respond to #success and #failure' do
          expect(listener).to receive(:respond_to?).and_return(false)
          expect { future.add_listener(listener) }.to raise_error(::ArgumentError, "listener must respond to both #success and #failure")
        end

        it 'calls listener.failure immediately with error' do
          expect(listener).to receive(:respond_to?).with(:success).and_return(true)
          expect(listener).to receive(:respond_to?).with(:failure).and_return(true)
          expect(listener).to receive(:failure).once.with(error)
          future.add_listener(listener)
        end

        it 'ignores errors raised by listener' do
          expect(listener).to receive(:respond_to?).with(:success).and_return(true)
          expect(listener).to receive(:respond_to?).with(:failure).and_return(true)
          expect(listener).to receive(:failure).once.with(error).and_raise
          future.add_listener(listener)
        end
      end

      describe('#then') do
        it 'raises if no block given' do
          expect { future.then }.to raise_error(ArgumentError, "no block given")
        end

        it 'returns self' do
          expect(future.then {|v| nil}).to eq(future)
        end
      end

      describe('#fallback') do
        it 'raises of no block given' do
          expect { future.fallback }.to raise_error(ArgumentError, "no block given")
        end

        it 'calls block immediately with error' do
          e = nil
          future.fallback {|error| e = error}
          expect(e).to eq(error)
        end

        context 'block returns value' do
          it 'returns a new value future' do
            future2 = future.fallback {|e| 5}
            expect(future2.get).to eq(5)
          end
        end

        context 'block returns a future' do
          it 'returns future' do
            future2 = future.fallback {|e| future}
            expect(future2).to eq(future)
          end
        end

        context 'block raises error' do
          it 'returns a new error future' do
            future2 = future.fallback {|e| raise "something bad"}
            expect { future2.get }.to raise_error("something bad")
          end
        end
      end
    end
  end
end
