# encoding: utf-8

require 'spec_helper'


module Cql
  describe Promise do
    describe '#succeed' do
    end

    describe '#fail' do

    end
  end

  describe Future do
    let :future do
      described_class.new
    end

    context 'when successful' do
      it 'is successful' do
        future.succeed('foo')
        future.should be_successful
      end

      it 'is completed' do
        future.succeed('foo')
        future.should be_completed
      end

      it 'has a value' do
        future.succeed('foo')
        future.value.should == 'foo'
      end

      it 'is successful even when the value is falsy' do
        future.succeed(nil)
        future.should be_successful
      end

      it 'returns the value from #get, too' do
        future.succeed('foo')
        future.get.should == 'foo'
      end

      it 'has the value nil by default' do
        future.succeed
        future.value.should be_nil
      end

      it 'notifies all success listeners' do
        v1, v2 = nil, nil
        future.on_success { |v| v1 = v }
        future.on_success { |v| v2 = v }
        future.succeed('bar')
        v1.should == 'bar'
        v2.should == 'bar'
      end

      it 'notifies all listeners even when one raises an error' do
        value = nil
        future.on_success { |v| raise 'Blurgh' }
        future.on_success { |v| value = v }
        future.succeed('bar')
        value.should == 'bar'
      end

      it 'notifies listeners registered after it became successful' do
        v1, v2 = nil, nil
        future.succeed('bar')
        future.on_success { |v| v1 = v }
        future.on_success { |v| v2 = v }
        v1.should == 'bar'
        v2.should == 'bar'
      end

      it 'does not raise any error when the listener raises an error when already successful' do
        future.succeed('bar')
        expect { future.on_success { |v| raise 'Blurgh' } }.to_not raise_error
      end

      it 'cannot be successful again' do
        future.succeed('bar')
        expect { future.succeed('foo') }.to raise_error(FutureError)
      end

      it 'cannot be failed' do
        future.succeed('bar')
        expect { future.fail(StandardError.new('FAIL!')) }.to raise_error(FutureError)
      end
    end

    context 'when failed' do
      it 'is failed' do
        future.fail(StandardError.new('FAIL!'))
        future.should be_failed
      end

      it 'is completed' do
        future.fail(StandardError.new('FAIL!'))
        future.should be_completed
      end

      it 'raises the error from #value' do
        future.fail(StandardError.new('FAIL!'))
        expect { future.value }.to raise_error('FAIL!')
      end

      it 'notifies all failure listeners' do
        e1, e2 = nil, nil
        future.on_failure { |e| e1 = e }
        future.on_failure { |e| e2 = e }
        future.fail(StandardError.new('FAIL!'))
        e1.message.should == 'FAIL!'
        e2.message.should == 'FAIL!'
      end

      it 'notifies all listeners even if one raises an error' do
        error = nil
        future.on_failure { |e| raise 'Blurgh' }
        future.on_failure { |e| error = e }
        future.fail(StandardError.new('FAIL!'))
        error.message.should == 'FAIL!'
      end

      it 'notifies new listeners even when already failed' do
        e1, e2 = nil, nil
        future.fail(StandardError.new('FAIL!'))
        future.on_failure { |e| e1 = e }
        future.on_failure { |e| e2 = e }
        e1.message.should == 'FAIL!'
        e2.message.should == 'FAIL!'
      end

      it 'does not raise any error when the listener raises an error when already failed' do
        future.fail(StandardError.new('FAIL!'))
        expect { future.on_failure { |e| raise 'Blurgh' } }.to_not raise_error
      end

      it 'cannot be failed again' do
        future.fail(StandardError.new('FAIL!'))
        expect { future.fail(StandardError.new('FAIL!')) }.to raise_error(FutureError)
      end

      it 'cannot be succeeded' do
        future.fail(StandardError.new('FAIL!'))
        expect { future.succeed('hurgh') }.to raise_error(FutureError)
      end
    end

    describe '#value' do
      it 'blocks on #value until successful' do
        Thread.start(future) do |f|
          sleep 0.1
          future.succeed('bar')
        end
        future.value.should == 'bar'
      end

      it 'blocks on #value until successful, when value is nil' do
        Thread.start(future) do |f|
          sleep 0.1
          future.succeed
        end
        future.value.should be_nil
        future.value.should be_nil
      end

      it 'blocks on #value until failed' do
        Thread.start(future) do |f|
          sleep 0.1
          future.fail(StandardError.new('FAIL!'))
        end
        expect { future.value }.to raise_error('FAIL!')
      end

      it 'allows multiple threads to block on #value until successful' do
        listeners = Array.new(10) do
          Thread.start do
            future.value
          end
        end
        sleep 0.1
        future.succeed(:hello)
        listeners.map(&:value).should == Array.new(10, :hello)
      end
    end

    describe '.combine' do
      context 'returns a new future which' do
        it 'is successful when the source futures are successful' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.combine(f1, f2)
          f1.succeed
          f3.should_not be_successful
          f2.succeed
          f3.should be_successful
        end

        it 'succeeds when the source futures succeed' do
          sequence = []
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          f4 = Future.combine(f1, f2, f3)
          f1.on_success { sequence << 1 } 
          f2.on_success { sequence << 2 } 
          f3.on_success { sequence << 3 }
          f2.succeed
          f1.succeed
          f3.succeed
          sequence.should == [2, 1, 3]
        end

        it 'returns an array of the values of the source futures, in order' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          f4 = Future.combine(f1, f2, f3)
          f2.succeed(2)
          f1.succeed(1)
          f3.succeed(3)
          f4.get.should == [1, 2, 3]
        end

        it 'fails if any of the source futures fail' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          f4 = Future.new
          f5 = Future.combine(f1, f2, f3, f4)
          f2.succeed
          f1.fail(StandardError.new('hurgh'))
          f3.fail(StandardError.new('murgasd'))
          f4.succeed
          expect { f5.get }.to raise_error('hurgh')
          f5.should be_failed
        end

        it 'raises an error when #succeed is called' do
          f = Future.combine(Future.new, Future.new)
          expect { f.succeed }.to raise_error(FutureError)
        end

        it 'raises an error when #fail is called' do
          f = Future.combine(Future.new, Future.new)
          expect { f.fail(StandardError.new('Blurgh')) }.to raise_error(FutureError)
        end

        it 'completes with an empty list when no futures are given' do
          Future.combine.get.should == []
        end
      end
    end

    describe '.first' do
      context 'it returns a new future which' do
        it 'is succceeded when the first of the source futures is successful' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          ff = Future.first(f1, f2, f3)
          f2.succeed
          ff.should be_successful
        end

        it 'succeeds with the value of the first source future' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          ff = Future.first(f1, f2, f3)
          f2.succeed('foo')
          ff.get.should == 'foo'
        end

        it 'is unaffected by the succeeding of the other futures' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          ff = Future.first(f1, f2, f3)
          f2.succeed
          f1.succeed
          f3.succeed
          ff.get
        end

        it 'is unaffected by a future failing when any other succeed' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          ff = Future.first(f1, f2, f3)
          f2.fail(StandardError.new('bork'))
          f1.fail(StandardError.new('bork'))
          f3.succeed
          expect { ff.get }.to_not raise_error
        end

        it 'fails if all of the source futures fail' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          ff = Future.first(f1, f2, f3)
          f2.fail(StandardError.new('bork'))
          f1.fail(StandardError.new('bork'))
          f3.fail(StandardError.new('bork'))
          ff.should be_failed
        end

        it 'fails with the error of the last future to fail' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          ff = Future.first(f1, f2, f3)
          f2.fail(StandardError.new('bork2'))
          f1.fail(StandardError.new('bork1'))
          f3.fail(StandardError.new('bork3'))
          expect { ff.get }.to raise_error('bork3')
        end
      end
    end

    describe '#map' do
      context 'returns a new future that' do
        it 'will succeed with the result of the given block' do
          mapped_value = nil
          f1 = Future.new
          f2 = f1.map { |v| v * 2 }
          f2.on_success { |v| mapped_value = v }
          f1.succeed(3)
          mapped_value.should == 3 * 2
        end

        it 'fails when the original future fails' do
          failed = false
          f1 = Future.new
          f2 = f1.map { |v| v * 2 }
          f2.on_failure { failed = true }
          f1.fail(StandardError.new('Blurgh'))
          failed.should be_true
        end

        it 'fails when the block raises an error' do
          f1 = Future.new
          f2 = f1.map { |v| raise 'Blurgh' }
          Thread.start do
            sleep(0.01)
            f1.succeed
          end
          expect { f2.get }.to raise_error('Blurgh')
        end
      end
    end

    describe '#flat_map' do
      it 'works like #map, but expects that the block returns a future' do
        f1 = Future.new
        f2 = f1.flat_map { |v| Future.successful(v * 2) }
        f1.succeed(3)
        f2.value.should == 3 * 2
      end

      it 'fails when the block raises an error' do
        f1 = Future.new
        f2 = f1.flat_map { |v| raise 'Hurgh' }
        f1.succeed(3)
        expect { f2.get }.to raise_error('Hurgh')
      end
    end

    describe '#recover' do
      context 'returns a new future that' do
        it 'succeeds with a value when the source future fails' do
          f1 = Future.new
          f2 = f1.recover { 'foo' }
          f1.fail(StandardError.new('Bork!'))
          f2.get.should == 'foo'
        end

        it 'yields the error to the block' do
          f1 = Future.new
          f2 = f1.recover { |e| e.message }
          f1.fail(StandardError.new('Bork!'))
          f2.get.should == 'Bork!'
        end

        it 'succeeds with the value of the source future when the source future is successful' do
          f1 = Future.new
          f2 = f1.recover { 'foo' }
          f1.succeed('bar')
          f2.get.should == 'bar'
        end

        it 'fails with the error raised in the given block' do
          f1 = Future.new
          f2 = f1.recover { raise 'Snork!' }
          f1.fail(StandardError.new('Bork!'))
          expect { f2.get }.to raise_error('Snork!')
        end
      end
    end

    describe '#fallback' do
      context 'returns a new future that' do
        it 'succeeds with the value of the fallback future when the source future fails' do
          f1 = Future.new
          f2 = Future.new
          f3 = f1.fallback { f2 }
          f1.fail(StandardError.new('Bork!'))
          f2.succeed('foo')
          f3.get.should == 'foo'
        end

        it 'yields the error to the block' do
          f1 = Future.new
          f2 = Future.new
          f3 = f1.fallback do |error|
            Future.successful(error.message)
          end
          f1.fail(StandardError.new('Bork!'))
          f3.get.should == 'Bork!'
        end

        it 'succeeds with the value of the source future when the source future succeeds' do
          f1 = Future.new
          f2 = Future.new
          f3 = f1.fallback { f2 }
          f2.succeed('bar')
          f1.succeed('foo')
          f3.get.should == 'foo'
        end

        it 'fails when the block raises an error' do
          f1 = Future.new
          f2 = f1.fallback { raise 'Bork!' }
          f1.fail(StandardError.new('Splork!'))
          expect { f2.get }.to raise_error('Bork!')
        end

        it 'fails when the fallback future fails' do
          f1 = Future.new
          f2 = Future.new
          f3 = f1.fallback { f2 }
          f2.fail(StandardError.new('Bork!'))
          f1.fail(StandardError.new('Fnork!'))
          expect { f3.get }.to raise_error('Bork!')
        end
      end
    end

    describe '.successful' do
      let :future do
        described_class.successful('hello world')
      end

      it 'is successful when created' do
        future.should be_successful
      end

      it 'calls callbacks immediately' do
        value = nil
        future.on_success { |v| value = v }
        value.should == 'hello world'
      end

      it 'does not block on #value' do
        future.value.should == 'hello world'
      end

      it 'defaults to the value nil' do
        described_class.successful.value.should be_nil
      end

      it 'handles #map' do
        described_class.successful('foo').map(&:upcase).value.should == 'FOO'
      end

      it 'handles #map when the map callback fails' do
        f = described_class.successful('foo').map { |v| raise 'Blurgh' }
        f.should be_failed
      end
    end

    describe '.failed' do
      let :future do
        described_class.failed(StandardError.new('Blurgh'))
      end

      it 'is failed when created' do
        future.should be_failed
      end

      it 'calls callbacks immediately' do
        error = nil
        future.on_failure { |e| error = e }
        error.message.should == 'Blurgh'
      end

      it 'does not block on #value' do
        expect { future.value }.to raise_error('Blurgh')
      end
    end
  end
end