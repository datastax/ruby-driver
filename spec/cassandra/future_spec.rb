require 'spec_helper'

module Cassandra
  describe(Future) do
    let(:signal) { double('promise signal') }
    let(:future) { Future.new(signal) }

    describe('#get') do
      it 'delegates to signal' do
        expect(signal).to receive(:get).once
        future.get
      end
    end

    describe('#join') do
      it 'delegates to signal' do
        expect(signal).to receive(:join).once
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
