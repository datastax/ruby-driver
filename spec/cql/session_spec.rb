# encoding: utf-8

require 'spec_helper'

module Cql
  describe(Session) do
    let(:client)  { double('cql-rb client') }
    let(:session) { Session.new(client) }

    describe('#execute_async') do
      context 'simple statement' do
        let(:statement) { 'SELECT * FROM songs' }

        it 'uses Client#execute' do
          promise = double('promise')

          expect(client).to receive(:execute).once.with(statement).and_return(promise)
          expect(session.execute_async(statement)).to eq(promise)
        end
      end

      context('prepared statement') do
        let(:statement) { Client::PreparedStatement.new }

        it 'uses PreparedStatement#execute' do
          promise = double('promise')

          expect(statement).to receive(:execute).once.with(no_args()).and_return(promise)
          expect(session.execute_async(statement)).to eq(promise)
        end
      end

      context('batch statement') do
        let(:statement) { Client::Batch.new }

        it 'uses Batch#execute' do
          promise = double('promise')

          expect(statement).to receive(:execute).once.with(no_args()).and_return(promise)
          expect(session.execute_async(statement)).to eq(promise)
        end
      end
    end

    describe('#prepare_async') do
      let(:statement) { 'SELECT * FROM songs' }

      it 'uses Client#prepare' do
        promise = double('promise')

        expect(client).to receive(:prepare).once.with(statement).and_return(promise)
        expect(session.prepare_async(statement)).to eq(promise)
      end
    end

    describe('#close_async') do
      it 'uses Client#close' do
        promise = double('promise')

        expect(client).to receive(:close).and_return(promise)
        expect(session.close_async).to eq(promise)
      end
    end

    [:batch, :logged_batch].each do |method|
      describe("##{method}") do
        it 'creates a logged batch' do
          batch = double('batch')

          expect(client).to receive(:batch).once.with(:logged).and_return(batch)
          expect(session.__send__(method)).to eq(batch)
        end
      end
    end

    describe('#unlogged_batch') do
      it 'creates an unlogged batch' do
        batch = double('batch')

        expect(client).to receive(:batch).once.with(:unlogged).and_return(batch)
        expect(session.unlogged_batch).to eq(batch)
      end
    end

    describe('#counter_batch') do
      it 'creates a counter batch' do
        batch = double('batch')

        expect(client).to receive(:batch).once.with(:counter).and_return(batch)
        expect(session.counter_batch).to eq(batch)
      end
    end

    [
      [:execute, ['statement to execute']],
      [:prepare, ['statement to prepare']],
      [:close,   []]
    ].each do |method, args|
      describe("##{method}") do
        let(:promise)   { double('promise') }
        before do
          expect(session).to receive(:"#{method}_async").with(*args).and_return(promise)
        end

        it "resolves a promise returned by ##{method}_async" do
          expect(promise).to receive(:get).once
          session.__send__(method, *args)
        end
      end
    end
  end
end
