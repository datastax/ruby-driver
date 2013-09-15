# encoding: utf-8

require 'spec_helper'


module Cql
  describe Promise do
    let :promise do
      described_class.new
    end

    let :future do
      promise.future
    end

    let :error do
      StandardError.new('bork')
    end

    describe '#fulfill' do
      it 'resolves its future' do
        promise.fulfill
        future.should be_resolved
      end

      it 'raises an error if fulfilled a second time' do
        promise.fulfill
        expect { promise.fulfill }.to raise_error(FutureError)
      end

      it 'raises an error if failed after being fulfilled' do
        promise.fulfill
        expect { promise.fail(error) }.to raise_error(FutureError)
      end

      it 'returns nil' do
        promise.fulfill(:foo).should be_nil
      end
    end

    describe '#fail' do
      it 'fails its future' do
        promise.fail(error)
        future.should be_failed
      end

      it 'raises an error if failed a second time' do
        promise.fail(error)
        expect { promise.fail(error) }.to raise_error(FutureError)
      end

      it 'raises an error if fulfilled after being failed' do
        promise.fail(error)
        expect { promise.fulfill }.to raise_error(FutureError)
      end

      it 'returns nil' do
        promise.fail(error).should be_nil
      end
    end

    describe '#observe' do
      it 'resolves its future when the specified future is resolved' do
        p2 = Promise.new
        promise.observe(p2.future)
        p2.fulfill
        promise.future.should be_resolved
      end

      it 'fails its future when the specified future fails' do
        p2 = Promise.new
        promise.observe(p2.future)
        p2.fail(error)
        promise.future.should be_failed
      end

      it 'silently ignores double fulfillment/failure' do
        p2 = Promise.new
        promise.observe(p2.future)
        promise.fail(error)
        p2.fulfill
      end

      it 'returns nil' do
        promise.observe(Promise.new.future).should be_nil
      end
    end

    describe '#try' do
      it 'fulfills the promise with the result of the block' do
        promise.try do
          3 + 4
        end
        promise.future.value.should == 7
      end

      it 'fails the promise when the block raises an error' do
        promise.try do
          raise error
        end
        expect { promise.future.value }.to raise_error(/bork/)
      end

      it 'calls the block with the specified arguments' do
        promise.try(:foo, 3) do |a, b|
          a.length + b
        end
        promise.future.value.should == 6
      end

      it 'returns nil' do
        promise.try { }.should be_nil
      end
    end
  end

  describe Future do
    let :promise do
      Promise.new
    end

    let :future do
      promise.future
    end

    let :error do
      StandardError.new('bork')
    end

    def async(*context, &listener)
      Thread.start(*context, &listener)
    end

    def delayed(*context, &listener)
      async(*context) do |*ctx|
        sleep(0.1)
        listener.call(*context)
      end
    end

    describe '#completed?' do
      it 'is true when the promise is fulfilled' do
        promise.fulfill
        future.should be_completed
      end

      it 'is true when the promise is failed' do
        promise.fail(StandardError.new('bork'))
        future.should be_completed
      end
    end

    describe '#resolved?' do
      it 'is true when the promise is fulfilled' do
        promise.fulfill('foo')
        future.should be_resolved
      end

      it 'is true when the promise is fulfilled with something falsy' do
        promise.fulfill(nil)
        future.should be_resolved
      end

      it 'is false when the promise is failed' do
        promise.fail(StandardError.new('bork'))
        future.should_not be_resolved
      end
    end

    describe '#failed?' do
      it 'is true when the promise is failed' do
        promise.fail(error)
        future.should be_failed
      end

      it 'is false when the promise is fulfilled' do
        promise.fulfill
        future.should_not be_failed
      end
    end

    describe '#on_complete' do
      context 'registers listeners and' do
        it 'notifies all listeners when the promise is fulfilled' do
          v1, v2 = nil, nil
          future.on_complete { |f| v1 = f.value }
          future.on_complete { |f| v2 = f.value }
          promise.fulfill('bar')
          v1.should == 'bar'
          v2.should == 'bar'
        end

        it 'notifies all listeners when the promise fails' do
          e1, e2 = nil, nil
          future.on_complete { |f| begin; f.value; rescue => err; e1 = err; end }
          future.on_complete { |f| begin; f.value; rescue => err; e2 = err; end }
          future.fail(error)
          e1.message.should == error.message
          e2.message.should == error.message
        end

        it 'notifies all listeners when the promise is fulfilled, even when one raises an error' do
          value = nil
          future.on_complete { |f| raise 'Blurgh' }
          future.on_complete { |f| value = f.value }
          promise.fulfill('bar')
          value.should == 'bar'
        end

        it 'notifies all listeners when the promise fails, even when one raises an error' do
          err = nil
          future.on_complete { |f| raise 'Blurgh' }
          future.on_complete { |f| begin; f.value; rescue => err; e = err; end }
          promise.fail(error)
          err.message.should == 'bork'
        end

        it 'notifies listeners registered after the promise was fulfilled' do
          promise.fulfill('bar')
          expect { future.on_complete { |v| raise 'blurgh' } }.to_not raise_error
        end

        it 'notifies listeners registered after the promise failed' do
          promise.fail(error)
          expect { future.on_complete { |v| raise 'blurgh' } }.to_not raise_error
        end

        it 'returns nil' do
          future.on_complete { :foo }.should be_nil
        end

        it 'returns nil when the future is already resolved' do
          promise.fulfill
          future.on_complete { :foo }.should be_nil
        end

        it 'returns nil when the future already has failed' do
          promise.fail(error)
          future.on_complete { :foo }.should be_nil
        end
      end
    end

    describe '#on_value' do
      context 'registers listeners and' do
        it 'notifies all value listeners when the promise is fulfilled' do
          v1, v2 = nil, nil
          future.on_value { |v| v1 = v }
          future.on_value { |v| v2 = v }
          promise.fulfill('bar')
          v1.should == 'bar'
          v2.should == 'bar'
        end

        it 'notifies all listeners even when one raises an error' do
          value = nil
          future.on_value { |v| raise 'Blurgh' }
          future.on_value { |v| value = v }
          promise.fulfill('bar')
          value.should == 'bar'
        end

        it 'notifies listeners registered after the promise was resolved' do
          v1, v2 = nil, nil
          promise.fulfill('bar')
          future.on_value { |v| v1 = v }
          future.on_value { |v| v2 = v }
          v1.should == 'bar'
          v2.should == 'bar'
        end

        it 'does not raise any error when the listener raises an error when already resolved' do
          promise.fulfill('bar')
          expect { future.on_value { |v| raise 'blurgh' } }.to_not raise_error
        end

        it 'returns nil' do
          future.on_value { :foo }.should be_nil
        end

        it 'returns nil when the future is already resolved' do
          promise.fulfill
          future.on_failure { :foo }.should be_nil
        end
      end
    end

    describe '#on_failure' do
      context 'registers listeners and' do
        it 'notifies all failure listeners when the promise fails' do
          e1, e2 = nil, nil
          future.on_failure { |err| e1 = err }
          future.on_failure { |err| e2 = err }
          promise.fail(error)
          e1.message.should eql(error.message)
          e2.message.should eql(error.message)
        end

        it 'notifies all listeners even if one raises an error' do
          e = nil
          future.on_failure { |err| raise 'Blurgh' }
          future.on_failure { |err| e = err }
          promise.fail(error)
          e.message.should eql(error.message)
        end

        it 'notifies new listeners even when already failed' do
          e1, e2 = nil, nil
          promise.fail(error)
          future.on_failure { |e| e1 = e }
          future.on_failure { |e| e2 = e }
          e1.message.should eql(error.message)
          e2.message.should eql(error.message)
        end

        it 'does not raise any error when the listener raises an error when already failed' do
          promise.fail(error)
          expect { future.on_failure { |e| raise 'Blurgh' } }.to_not raise_error
        end

        it 'returns nil' do
          future.on_failure { :foo }.should be_nil
        end

        it 'returns nil when the future already has failed' do
          promise.fail(error)
          future.on_failure { :foo }.should be_nil
        end
      end
    end

    describe '#value' do
      it 'is nil by default' do
        promise.fulfill
        future.value.should be_nil
      end

      it 'is the object passed to Promise#fulfill' do
        obj = 'hello world'
        promise.fulfill(obj)
        future.value.should equal(obj)
      end

      it 'raises the error passed to Promise#fail' do
        promise.fail(StandardError.new('bork'))
        expect { future.value }.to raise_error(/bork/)
      end

      it 'blocks until the promise is completed' do
        d = delayed(promise) do |p|
          p.fulfill('bar')
        end
        d.value
        future.value.should == 'bar'
      end

      it 'blocks on #value until fulfilled, when value is nil' do
        d = delayed(promise) do |p|
          p.fulfill
        end
        d.value
        future.value.should be_nil
      end

      it 'blocks on #value until failed' do
        d = delayed(promise) do |p|
          p.fail(StandardError.new('bork'))
        end
        d.value
        expect { future.value }.to raise_error('bork')
      end

      it 'allows multiple threads to block on #value until fulfilled' do
        listeners = Array.new(10) do
          async(future) do |f|
            f.value
          end
        end
        sleep 0.1
        promise.fulfill(:hello)
        listeners.map(&:value).should == Array.new(10, :hello)
      end
    end

    describe '#map' do
      context 'returns a new future that' do
        it 'will be fulfilled with the result of the given block' do
          mapped_value = nil
          p = Promise.new
          f = p.future.map { |v| v * 2 }
          f.on_value { |v| mapped_value = v }
          p.fulfill(3)
          mapped_value.should == 3 * 2
        end

        it 'fails when the original future fails' do
          failed = false
          p = Promise.new
          f = p.future.map { |v| v * 2 }
          f.on_failure { failed = true }
          p.fail(StandardError.new('Blurgh'))
          failed.should be_true
        end

        it 'fails when the block raises an error' do
          p = Promise.new
          f = p.future.map { |v| raise 'blurgh' }
          d = delayed do
            p.fulfill
          end
          d.value
          expect { f.value }.to raise_error('blurgh')
        end
      end
    end

    describe '#flat_map' do
      it 'works like #map, but expects that the block returns a future' do
        p = Promise.new
        f = p.future.flat_map { |v| Future.resolved(v * 2) }
        p.fulfill(3)
        f.value.should == 3 * 2
      end

      it 'fails when the block raises an error' do
        p = Promise.new
        f = p.future.flat_map { |v| raise 'Hurgh' }
        p.fulfill(3)
        expect { f.value }.to raise_error('Hurgh')
      end
    end

    describe '#recover' do
      context 'returns a new future that' do
        it 'becomes fulfilled with a value when the source future fails' do
          p = Promise.new
          f = p.future.recover { 'foo' }
          p.fail(error)
          f.value.should == 'foo'
        end

        it 'yields the error to the block' do
          p = Promise.new
          f = p.future.recover { |e| e.message }
          p.fail(error)
          f.value.should == error.message
        end

        it 'becomes fulfilled with the value of the source future when the source future is fulfilled' do
          p = Promise.new
          f = p.future.recover { 'foo' }
          p.fulfill('bar')
          f.value.should == 'bar'
        end

        it 'fails with the error raised in the given block' do
          p = Promise.new
          f = p.future.recover { raise 'snork' }
          p.fail(StandardError.new('bork'))
          expect { f.value }.to raise_error('snork')
        end
      end
    end

    describe '#fallback' do
      context 'returns a new future that' do
        it 'is resolved with the value of the fallback future when the source future fails' do
          p1 = Promise.new
          p2 = Promise.new
          f = p1.future.fallback { p2.future }
          p1.fail(error)
          p2.fulfill('foo')
          f.value.should == 'foo'
        end

        it 'yields the error to the block' do
          p1 = Promise.new
          p2 = Promise.new
          f = p1.future.fallback do |error|
            Future.resolved(error.message)
          end
          p1.fail(error)
          f.value.should == error.message
        end

        it 'is resolved with the value of the source future when the source future fullfills' do
          p1 = Promise.new
          p2 = Promise.new
          f = p1.future.fallback { p2.future }
          p2.fulfill('bar')
          p1.fulfill('foo')
          f.value.should == 'foo'
        end

        it 'fails when the block raises an error' do
          p = Promise.new
          f = p.future.fallback { raise 'bork' }
          p.fail(StandardError.new('splork'))
          expect { f.value }.to raise_error('bork')
        end

        it 'fails when the fallback future fails' do
          p1 = Promise.new
          p2 = Promise.new
          f = p1.future.fallback { p2.future }
          p2.fail(StandardError.new('bork'))
          p1.fail(StandardError.new('fnork'))
          expect { f.value }.to raise_error('bork')
        end
      end
    end

    describe '.all' do
      context 'returns a new future which' do
        it 'is resolved when the source futures are resolved' do
          p1 = Promise.new
          p2 = Promise.new
          f = Future.all(p1.future, p2.future)
          p1.fulfill
          f.should_not be_resolved
          p2.fulfill
          f.should be_resolved
        end

        it 'resolves when the source futures are resolved' do
          sequence = []
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          f = Future.all(p1.future, p2.future, p3.future)
          p1.future.on_value { sequence << 1 } 
          p2.future.on_value { sequence << 2 } 
          p3.future.on_value { sequence << 3 }
          p2.fulfill
          p1.fulfill
          p3.fulfill
          sequence.should == [2, 1, 3]
        end

        it 'returns an array of the values of the source futures, in order ' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          f = Future.all(p1.future, p2.future, p3.future)
          p2.fulfill(2)
          p1.fulfill(1)
          p3.fulfill(3)
          f.value.should == [1, 2, 3]
        end

        it 'fails if any of the source futures fail' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          p4 = Promise.new
          f = Future.all(p1.future, p2.future, p3.future, p4.future)
          p2.fulfill
          p1.fail(StandardError.new('hurgh'))
          p3.fail(StandardError.new('murgasd'))
          p4.fulfill
          expect { f.value }.to raise_error('hurgh')
          f.should be_failed
        end

        it 'completes with an empty list when no futures are given' do
          Future.all.value.should == []
        end
      end
    end

    describe '.first' do
      context 'it returns a new future which' do
        it 'is resolved when the first of the source futures is resolved' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          f = Future.first(p1.future, p2.future, p3.future)
          p2.fulfill
          f.should be_resolved
        end

        it 'fullfills with the value of the first source future' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          f = Future.first(p1.future, p2.future, p3.future)
          p2.fulfill('foo')
          f.value.should == 'foo'
        end

        it 'is unaffected by the fullfillment of the other futures' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          f = Future.first(p1.future, p2.future, p3.future)
          p2.fulfill
          p1.fulfill
          p3.fulfill
          f.value
        end

        it 'is unaffected by a future failing when at least one resolves' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          f = Future.first(p1.future, p2.future, p3.future)
          p2.fail(error)
          p1.fail(error)
          p3.fulfill
          expect { f.value }.to_not raise_error
        end

        it 'fails if all of the source futures fail' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          f = Future.first(p1.future, p2.future, p3.future)
          p2.fail(error)
          p1.fail(error)
          p3.fail(error)
          f.should be_failed
        end

        it 'fails with the error of the last future to fail' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          f = Future.first(p1.future, p2.future, p3.future)
          p2.fail(StandardError.new('bork2'))
          p1.fail(StandardError.new('bork1'))
          p3.fail(StandardError.new('bork3'))
          expect { f.value }.to raise_error('bork3')
        end

        it 'completes with nil when no futures are given' do
          Future.first.value.should be_nil
        end
      end
    end

    describe '.resolved' do
      context 'returns a future which' do
        let :future do
          described_class.resolved('hello world')
        end

        it 'returns a future which is resolved' do
          future.should be_resolved
        end

        it 'calls callbacks immediately' do
          value = nil
          future.on_value { |v| value = v }
          value.should == 'hello world'
        end

        it 'does not block on #value' do
          future.value.should == 'hello world'
        end

        it 'defaults to the value nil' do
          described_class.resolved.value.should be_nil
        end
      end
    end

    describe '.failed' do
      let :future do
        described_class.failed(error)
      end

      context 'returns a future which' do
        it 'is failed when created' do
          future.should be_failed
        end

        it 'calls callbacks immediately' do
          error = nil
          future.on_failure { |e| error = e }
          error.message.should == 'bork'
        end

        it 'does not block on #value' do
          expect { future.value }.to raise_error('bork')
        end
      end
    end
  end
end