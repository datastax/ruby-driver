require 'spec_helper'

module Cassandra
  class Promise
    describe(Signal) do
      let(:signal) { Signal.new(Executors::SameThread.new) }

      describe('#failure') do
        it 'raises if error is not an Exception' do
          error = double('error')
          expect { signal.failure(error) }.to raise_error(ArgumentError, "error must be an exception, #{error.inspect} given")
        end

        let(:error) { RuntimeError.new('some error') }

        it 'notifies all listeners' do
          listener = double('listener')
          signal.add_listener(listener)
          expect(listener).to receive(:failure).once.with(error)
          signal.failure(error)
        end

        it 'makes #get raise error' do
          signal.failure(error)
          expect { signal.get }.to raise_error('some error')
        end

        context 'signal is succeeded' do
          let(:value) { double('some value') }

          before do
            signal.success(value)
          end

          it 'is ignored' do
            signal.failure(error)
            expect(signal.get).to eq(value)
          end
        end
      end

      describe('#success') do
        let(:value) { double('some value') }

        it 'notifies all listeners' do
          listener = double('listener')
          signal.add_listener(listener)
          expect(listener).to receive(:success).once.with(value)
          signal.success(value)
        end

        it 'makes #get return value' do
          signal.success(value)
          expect(signal.get).to eq(value)
        end

        context 'signal is failed' do
          let(:error) { RuntimeError.new('some error') }

          before do
            signal.failure(error)
          end

          it 'is ignored' do
            signal.success(value)
            expect { signal.get }.to raise_error('some error')
          end
        end
      end

      describe('#join') do
        it 'blocks until failure' do
          resolved = false
          thread   = Thread.new { signal.join; resolved = true }
          sleep(0.001)
          expect(resolved).to be_falsey
          signal.failure(RuntimeError.new)
          thread.join(1)
          expect(resolved).to be_truthy
        end

        it 'blocks until success' do
          resolved = false
          thread   = Thread.new { signal.join; resolved = true }
          sleep(0.001)
          expect(resolved).to be_falsey
          signal.success(double('some value'))
          thread.join(1)
          expect(resolved).to be_truthy
        end
      end

      describe('#get') do
        context 'when it is resolved with error' do
          let(:error) { RuntimeError.new('some error') }

          before do
            signal.failure(error)
          end

          it 'raises error' do
            expect { signal.get }.to raise_error('some error')
          end
        end

        context 'when it is resolved with value' do
          let(:value) { double('some value') }

          before do
            signal.success(value)
          end

          it 'raises error' do
            expect(signal.get).to eq(value)
          end
        end

        context 'when it is not resolved' do
          it 'blocks until resolved' do
            resolved = false
            thread   = Thread.new { signal.join; resolved = true }
            sleep(0.001)
            expect(resolved).to be_falsey
            signal.success(double('some value'))
            thread.join(1)
            expect(resolved).to be_truthy
          end
        end
      end

      describe('#add_listener') do
        let(:listener) { double('listener') }

        context 'when it is resolved with error' do
          let(:error) { RuntimeError.new('some error') }

          before do
            signal.failure(error)
          end

          it 'calls listener#failure immediately' do
            expect(listener).to receive(:failure).once.with(error)
            signal.add_listener(listener)
          end
        end

        context 'when it is resolved with value' do
          let(:value) { double('some value') }

          before do
            signal.success(value)
          end

          it 'calls listener#success immediately' do
            expect(listener).to receive(:success).once.with(value)
            signal.add_listener(listener)
          end
        end

        context 'when it is not resolved' do
          it 'adds listener' do
            expect(listener).to_not receive(:success)
            expect(listener).to_not receive(:failure)
            signal.add_listener(listener)
          end
        end
      end

      describe('#on_success') do
        context 'when it is resolved with error' do
          let(:error) { RuntimeError.new('some error') }

          before do
            signal.failure(error)
          end

          it 'ignores block' do
            c = false
            v = nil

            signal.on_success {|value| c = true; v = value}
            expect(c).to be_falsey
            expect(v).to be_nil
          end
        end

        context 'when it is resolved with value' do
          let(:value) { double('some value') }

          before do
            signal.success(value)
          end

          it 'calls block immediately with value' do
            c = false
            v = nil

            signal.on_success {|value| c = true; v = value}
            expect(c).to be_truthy
            expect(v).to eq(value)
          end
        end

        context 'when it is not resolved' do
          it 'adds block' do
            c = false
            v = nil

            signal.on_success {|value| c = true; v = value}
            expect(c).to be_falsey
            expect(v).to be_nil
          end
        end
      end

      describe('#on_failure') do
        context 'when it is resolved with error' do
          let(:error) { RuntimeError.new('some error') }

          before do
            signal.failure(error)
          end

          it 'calls block immediately with error' do
            c = false
            e = nil

            signal.on_failure {|error| c = true; e = error}
            expect(c).to be_truthy
            expect(e).to eq(error)
          end
        end

        context 'when it is resolved with value' do
          let(:value) { double('some value') }

          before do
            signal.success(value)
          end

          it 'ignores block' do
            c = false
            e = nil

            signal.on_failure {|error| c = true; e = error}
            expect(c).to be_falsey
            expect(e).to be_nil
          end
        end

        context 'when it is not resolved' do
          it 'adds block' do
            c = false
            e = nil

            signal.on_failure {|error| c = true; e = error}
            expect(c).to be_falsey
            expect(e).to be_nil
          end
        end
      end

      describe('#on_complete') do
        context 'when it is resolved with error' do
          let(:error) { RuntimeError.new('some error') }

          before do
            signal.failure(error)
          end

          it 'calls block immediately with error and no value' do
            c = false
            v = nil
            e = nil

            signal.on_complete {|value, error| c = true; v = value; e = error}
            expect(c).to be_truthy
            expect(v).to be_nil
            expect(e).to eq(error)
          end
        end

        context 'when it is resolved with value' do
          let(:value) { double('some value') }

          before do
            signal.success(value)
          end

          it 'calls block immediately with value and no error' do
            c = false
            v = nil
            e = nil

            signal.on_complete {|value, error| c = true; v = value; e = error}
            expect(c).to be_truthy
            expect(v).to eq(value)
            expect(e).to be_nil
          end
        end

        context 'when it is not resolved' do
          it 'adds block' do
            c = false
            v = nil
            e = nil

            signal.on_complete {|value, error| c = true; v = value; e = error}
            expect(v).to be_nil
            expect(c).to be_falsey
            expect(e).to be_nil
          end
        end
      end

      describe('#then') do
        context 'when it is resolved with error' do
          let(:error) { RuntimeError.new('some error') }

          before do
            signal.failure(error)
          end

          it 'ignores block and returns a future error' do
            future = signal.then {|v| 10}
            expect { future.get }.to raise_error('some error')
          end
        end

        context 'when it is resolved with value' do
          let(:value) { double('some value') }

          before do
            signal.success(value)
          end

          context 'and block returns a value' do
            it 'calls block with value and returns a future value' do
              future = signal.then {|v| v == value }
              expect(future.get).to be_truthy
            end
          end

          context 'and block returns a future' do
            it 'calls block with value and returns the future' do
              future = signal.then {|v| Future.value(value) }
              expect(future.get).to eq(value)
            end
          end
        end

        context 'when it is not resolved' do
          it 'returns a future that resolves later' do
            resolved = false
            future   = signal.then {|v| 'some value'}
            thread   = Thread.new { resolved = (future.get == 'some value') }
            sleep(0.001)
            expect(resolved).to be_falsey
            signal.success(nil)
            thread.join(1)
            expect(resolved).to be_truthy
          end
        end
      end

      describe('#fallback') do
        context 'when it is resolved with error' do
          let(:error) { RuntimeError.new('some error') }

          before do
            signal.failure(error)
          end

          context 'and block returns a value' do
            it 'calls block with error and returns a future value' do
              future = signal.fallback {|e| e == error }
              expect(future.get).to be_truthy
            end
          end

          context 'and block returns a future' do
            it 'calls block with error and returns the future' do
              future = signal.fallback {|e| Future.value(nil) }
              expect(future.get).to be_nil
            end
          end
        end

        context 'when it is resolved with value' do
          let(:value) { double('some value') }

          before do
            signal.success(value)
          end

          it 'ignores block and returns a future value' do
            future = signal.fallback {|e| 10}
            expect(future.get).to eq(value)
          end
        end

        context 'when it is not resolved' do
          it 'returns a future that resolves later' do
            resolved = false
            future   = signal.fallback {|e| 'some value'}
            thread   = Thread.new { resolved = (future.get == 'some value') }
            sleep(0.001)
            expect(resolved).to be_falsey
            signal.failure(RuntimeError.new)
            thread.join(1)
            expect(resolved).to be_truthy
          end
        end
      end
    end
  end
end
