# encoding: utf-8

require 'spec_helper'


module Cql
  describe Future do
    let :future do
      described_class.new
    end

    context 'when completed' do
      it 'is complete' do
        future.complete!('foo')
        future.should be_complete
      end

      it 'has a value' do
        future.complete!('foo')
        future.value.should == 'foo'
      end

      it 'is complete even when the value is falsy' do
        future.complete!(nil)
        future.should be_complete
      end

      it 'returns the value from #get, too' do
        future.complete!('foo')
        future.get.should == 'foo'
      end

      it 'has the value nil by default' do
        future.complete!
        future.value.should be_nil
      end

      it 'notifies all completion listeners' do
        v1, v2 = nil, nil
        future.on_complete { |v| v1 = v }
        future.on_complete { |v| v2 = v }
        future.complete!('bar')
        v1.should == 'bar'
        v2.should == 'bar'
      end

      it 'notifies new listeners even when already completed' do
        v1, v2 = nil, nil
        future.complete!('bar')
        future.on_complete { |v| v1 = v }
        future.on_complete { |v| v2 = v }
        v1.should == 'bar'
        v2.should == 'bar'
      end

      it 'blocks on #value until completed' do
        Thread.start(future) do |f|
          sleep 0.1
          future.complete!('bar')
        end
        future.value.should == 'bar'
      end

      it 'blocks on #value until completed, when value is nil' do
        Thread.start(future) do |f|
          sleep 0.1
          future.complete!
        end
        future.value.should be_nil
        future.value.should be_nil
      end

      it 'blocks on #value until failed' do
        Thread.start(future) do |f|
          sleep 0.1
          future.fail!(StandardError.new('FAIL!'))
        end
        expect { future.value }.to raise_error('FAIL!')
      end

      it 'cannot be completed again' do
        future.complete!('bar')
        expect { future.complete!('foo') }.to raise_error(FutureError)
      end

      it 'cannot be failed again' do
        future.complete!('bar')
        expect { future.fail!(StandardError.new('FAIL!')) }.to raise_error(FutureError)
      end
    end

    context 'when failed' do
      it 'is failed' do
        future.fail!(StandardError.new('FAIL!'))
        future.should be_failed
      end

      it 'raises the error from #value' do
        future.fail!(StandardError.new('FAIL!'))
        expect { future.value }.to raise_error('FAIL!')
      end

      it 'notifies all failure listeners' do
        e1, e2 = nil, nil
        future.on_failure { |e| e1 = e }
        future.on_failure { |e| e2 = e }
        future.fail!(StandardError.new('FAIL!'))
        e1.message.should == 'FAIL!'
        e2.message.should == 'FAIL!'
      end

      it 'notifies new listeners even when already failed' do
        e1, e2 = nil, nil
        future.fail!(StandardError.new('FAIL!'))
        future.on_failure { |e| e1 = e }
        future.on_failure { |e| e2 = e }
        e1.message.should == 'FAIL!'
        e2.message.should == 'FAIL!'
      end

      it 'cannot be failed again' do
        future.fail!(StandardError.new('FAIL!'))
        expect { future.fail!(StandardError.new('FAIL!')) }.to raise_error(FutureError)
      end

      it 'cannot be completed' do
        future.fail!(StandardError.new('FAIL!'))
        expect { future.complete!('hurgh') }.to raise_error(FutureError)
      end
    end

    describe '.combine' do
      context 'returns a new future which' do
        it 'is complete when the source futures are complete' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.combine(f1, f2)
          f1.complete!
          f3.should_not be_complete
          f2.complete!
          f3.should be_complete
        end

        it 'completes when the source futures have completed' do
          sequence = []
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          f4 = Future.combine(f1, f2, f3)
          f1.on_complete { sequence << 1 } 
          f2.on_complete { sequence << 2 } 
          f3.on_complete { sequence << 3 }
          f2.complete!
          f1.complete!
          f3.complete!
          sequence.should == [2, 1, 3]
        end

        it 'returns an array of the values of the source futures, in order' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          f4 = Future.combine(f1, f2, f3)
          f2.complete!(2)
          f1.complete!(1)
          f3.complete!(3)
          f4.get.should == [1, 2, 3]
        end

        it 'fails if any of the source futures fail' do
          f1 = Future.new
          f2 = Future.new
          f3 = Future.new
          f4 = Future.new
          f5 = Future.combine(f1, f2, f3, f4)
          f2.complete!
          f1.fail!(StandardError.new('hurgh'))
          f3.fail!(StandardError.new('murgasd'))
          f4.complete!
          expect { f5.get }.to raise_error('hurgh')
          f5.should be_failed
        end

        it 'raises an error when #complete! is called' do
          f = Future.combine(Future.new, Future.new)
          expect { f.complete! }.to raise_error(FutureError)
        end

        it 'raises an error when #fail! is called' do
          f = Future.combine(Future.new, Future.new)
          expect { f.fail!(StandardError.new('Blurgh')) }.to raise_error(FutureError)
        end
      end
    end

    describe '#map' do
      context 'returns a new future that' do
        it 'will complete with the result of the given block' do
          mapped_value = nil
          f1 = Future.new
          f2 = f1.map { |v| v * 2 }
          f2.on_complete { |v| mapped_value = v }
          f1.complete!(3)
          mapped_value.should == 3 * 2
        end

        it 'fails when the original future fails' do
          failed = false
          f1 = Future.new
          f2 = f1.map { |v| v * 2 }
          f2.on_failure { failed = true }
          f1.fail!(StandardError.new('Blurgh'))
          failed.should be_true
        end

        it 'fails when the block raises an error' do
          f1 = Future.new
          f2 = f1.map { |v| raise 'Blurgh' }
          Thread.start do
            sleep(0.01)
            f1.complete!
          end
          expect { f2.get }.to raise_error('Blurgh')
        end
      end
    end

    describe '#flat_map' do
      it 'works like #map, but expects that the block returns a future' do
        f1 = Future.new
        f2 = f1.flat_map { |v| CompletedFuture.new(v * 2) }
        f1.complete!(3)
        f2.value.should == 3 * 2
      end
    end
  end

  describe CompletedFuture do
    let :future do
      described_class.new('hello world')
    end

    it 'is complete when created' do
      future.should be_complete
    end

    it 'calls callbacks immediately' do
      value = nil
      future.on_complete { |v| value = v }
      value.should == 'hello world'
    end

    it 'does not block on #value' do
      future.value.should == 'hello world'
    end

    it 'defaults to the value nil' do
      described_class.new.value.should be_nil
    end

    it 'handles #map' do
      described_class.new('foo').map(&:upcase).value.should == 'FOO'
    end

    it 'handles #map' do
      f = described_class.new('foo').map { |v| raise 'Blurgh' }
      f.should be_failed
    end
  end

  describe FailedFuture do
    let :future do
      described_class.new(StandardError.new('Blurgh'))
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