# encoding: utf-8

require 'spec_helper'

module Cql
  describe(Session) do
    let(:default_options) { {:consistency => :one, :timeout => 5, :trace => false} }
    let(:session_options) { Session::Options.new(default_options) }
    let(:client)          { double('cql-rb client') }
    let(:session)         { Session.new(client, session_options) }

    describe('#execute_async') do
      context 'cql string' do
        context 'without parameters' do
          let(:cql) { 'SELECT * FROM songs' }

          it 'sends query with a simple statement' do
            promise   = double('promise')
            statement = double('simple statement')

            expect(Statements::Simple).to receive(:new).once.with(cql, []).and_return(statement)
            expect(client).to receive(:query).once.with(statement, session_options).and_return(promise)
            expect(session.execute_async(cql)).to eq(promise)
          end
        end

        context 'with parameters' do
          let(:cql) { 'SELECT * FROM songs LIMIT ?' }

          it 'sends query with a simple statement with parameters' do
            promise   = double('promise')
            statement = double('simple statement')

            expect(Statements::Simple).to receive(:new).once.with(cql, [1]).and_return(statement)
            expect(client).to receive(:query).once.with(statement, session_options).and_return(promise)
            expect(session.execute_async(cql, 1)).to eq(promise)
          end
        end

        context 'with options' do
          let(:cql) { 'SELECT * FROM songs' }
          let(:options) { {:trace => true} }

          it 'merges default options with options supplied' do
            promise   = double('promise')
            statement = double('simple statement')
            opts      = double('options')

            expect(session_options).to receive(:override).once.with(options).and_return(opts)
            expect(Statements::Simple).to receive(:new).once.with(cql, []).and_return(statement)
            expect(client).to receive(:query).once.with(statement, opts).and_return(promise)
            expect(session.execute_async(cql, options)).to eq(promise)
          end
        end
      end

      context('prepared statement') do
        let(:cql)             { "INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)" }
        let(:result_metadata) { nil }
        let(:params_metadata) { Array.new(5) }
        let(:statement)       { Statements::Prepared.new(cql, params_metadata, result_metadata) }

        it 'binds and executes result' do
          promise         = double('promise')
          bound_statement = double('bound statement')
          options         = double('options')

          expect(Session::Options).to receive(:new).once.with(default_options).and_return(options)
          expect(statement).to receive(:bind).with(1, 2, 3, 4, 5).and_return(bound_statement)
          expect(client).to receive(:execute).once.with(bound_statement, options).and_return(promise)
          expect(session.execute_async(statement, 1, 2, 3, 4, 5)).to eq(promise)
        end
      end

      context('bound statement') do
        let(:cql)             { "INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)" }
        let(:result_metadata) { nil }
        let(:params_metadata) { Array.new(5) }
        let(:params)          { [1,2,3,4,5] }
        let(:statement)       { Statements::Bound.new(cql, params_metadata, result_metadata, params) }

        it 'executes statement' do
          promise         = double('promise')
          bound_statement = double('bound statement')
          options         = double('options')

          expect(Session::Options).to receive(:new).once.with(default_options).and_return(options)
          expect(client).to receive(:execute).once.with(statement, options).and_return(promise)
          expect(session.execute_async(statement)).to eq(promise)
        end
      end

      context('batch statement') do
        let(:statement) { Statements::Batch::Logged.new }

        it 'sends batch to the client' do
          promise = double('promise')
          options = double('options')

          expect(Session::Options).to receive(:new).once.with(default_options).and_return(options)
          expect(client).to receive(:batch).once.with(statement, options).and_return(promise)
          expect(session.execute_async(statement)).to eq(promise)
        end
      end
    end

    describe('#prepare_async') do
      let(:cql) { 'SELECT * FROM songs' }

      context 'without options' do
        it 'prepares cql with the client' do
          promise = double('promise')

          expect(client).to receive(:prepare).once.with(cql, session_options).and_return(promise)
          expect(session.prepare_async(cql)).to eq(promise)
        end
      end

      context 'with options' do
        let(:options) { {:trace => true} }

        it 'sends options to the client when preparing' do
          promise = double('promise')
          opts    = double('options')

          expect(session_options).to receive(:override).once.with(options).and_return(opts)
          expect(client).to receive(:prepare).once.with(cql, opts).and_return(promise)
          expect(session.prepare_async(cql, options)).to eq(promise)
        end
      end

      context 'with simple statement' do
        let(:statement) { Statements::Simple.new(cql) }

        it 'prepares using cql' do
          promise = double('promise')

          expect(client).to receive(:prepare).once.with(cql, session_options).and_return(promise)
          expect(session.prepare_async(statement)).to eq(promise)
        end
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

          expect(Statements::Batch::Logged).to receive(:new).once.and_return(batch)
          expect(session.__send__(method)).to eq(batch)
        end
      end
    end

    describe('#unlogged_batch') do
      it 'creates an unlogged batch' do
        batch = double('batch')

        expect(Statements::Batch::Unlogged).to receive(:new).once.and_return(batch)
        expect(session.unlogged_batch).to eq(batch)
      end
    end

    describe('#counter_batch') do
      it 'creates a counter batch' do
        batch = double('batch')

        expect(Statements::Batch::Counter).to receive(:new).once.and_return(batch)
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
