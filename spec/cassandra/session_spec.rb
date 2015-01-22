# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
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
  describe(Session) do
    let(:default_options) { {:consistency => :one, :timeout => 5, :trace => false} }
    let(:session_options) { Execution::Options.new(default_options) }
    let(:client)          { double('cassandra-driver') }
    let(:session)         { Session.new(client, session_options, Future::Factory.new(Executors::SameThread.new)) }

    describe('#execute_async') do
      context 'cql string' do
        context 'without arguments' do
          let(:cql) { 'SELECT * FROM songs' }

          it 'sends query with a simple statement' do
            promise   = double('promise')
            statement = double('simple statement')

            expect(Statements::Simple).to receive(:new).once.with(cql, EMPTY_LIST).and_return(statement)
            expect(client).to receive(:query).once.with(statement, session_options).and_return(promise)
            expect(session.execute_async(cql)).to eq(promise)
          end
        end

        context 'with arguments' do
          let(:cql) { 'SELECT * FROM songs LIMIT ?' }

          it 'sends query with a simple statement with parameters' do
            promise   = double('promise')
            statement = double('simple statement')

            expect(Statements::Simple).to receive(:new).once.with(cql, [1]).and_return(statement)
            expect(client).to receive(:query).once.with(statement, session_options.override(arguments: [1])).and_return(promise)
            expect(session.execute_async(cql, arguments: [1])).to eq(promise)
          end
        end

        context 'with options' do
          let(:cql) { 'SELECT * FROM songs' }
          let(:options) { {:trace => true} }

          it 'merges default options with options supplied' do
            promise   = double('promise')
            statement = double('simple statement')

            expect(Statements::Simple).to receive(:new).once.with(cql, EMPTY_LIST).and_return(statement)
            expect(client).to receive(:query).once.with(statement, session_options.override(options)).and_return(promise)
            expect(session.execute_async(cql, options)).to eq(promise)
          end
        end
      end

      context('prepared statement') do
        let(:cql)             { "INSERT INTO songs (id, title, album, artist, tags) VALUES (?, ?, ?, ?, ?)" }
        let(:result_metadata) { nil }
        let(:params_metadata) { Array.new(5) }
        let(:statement)       { Statements::Prepared.new(cql, params_metadata, result_metadata, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil) }

        it 'binds and executes result' do
          promise         = double('promise')
          bound_statement = double('bound statement')
          options         = double('options')

          expect(session_options).to receive(:override).once.with(arguments: [1, 2, 3, 4, 5]).and_return(options)
          allow(options).to receive(:arguments).and_return([1, 2, 3, 4, 5])
          expect(statement).to receive(:bind).with([1, 2, 3, 4, 5]).and_return(bound_statement)
          expect(client).to receive(:execute).once.with(bound_statement, options).and_return(promise)
          expect(session.execute_async(statement, arguments: [1, 2, 3, 4, 5])).to eq(promise)
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

          expect(Execution::Options).to receive(:new).once.with(default_options).and_return(options)
          expect(client).to receive(:execute).once.with(statement, options).and_return(promise)
          expect(session.execute_async(statement)).to eq(promise)
        end
      end

      context('batch statement') do
        let(:statement) { Statements::Batch::Logged.new }

        it 'sends batch to the client' do
          promise = double('promise')
          options = double('options')

          expect(Execution::Options).to receive(:new).once.with(default_options).and_return(options)
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
      let(:promise) { double('promise').as_null_object }

      before do
        expect(Promise).to receive(:new).and_return(promise)
      end

      it 'uses Client#close' do
        expect(client).to receive(:close).and_return(Ione::Future.resolved)
        expect(session.close_async).to eq(promise)
        expect(promise).to have_received(:fulfill).once.with(session)
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

    describe('#prepare') do
      let(:promise)   { double('promise') }
      let(:args)      { [double('statement to prepare')] }

      it "resolves a promise returned by #prepare_async" do
        expect(session).to receive(:prepare_async).with(*args).and_return(promise)
        expect(promise).to receive(:get).once

        session.prepare(*args)
      end
    end

    describe('#close') do
      let(:promise)   { double('promise') }

      it 'resolves a promise returned by #close_async' do
        expect(session).to receive(:close_async).with(no_args).and_return(promise)
        expect(promise).to receive(:get).once.and_return('success')
        expect(session.close).to eq('success')
      end
    end
  end
end
