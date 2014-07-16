# encoding: utf-8

require 'spec_helper'

module Cql
  describe(Cluster) do
    let(:io_reactor)         { FakeIoReactor.new }
    let(:control_connection) { double('control connection') }
    let(:cluster_registry)   { FakeClusterRegistry.new(['1.1.1.1', '2.2.2.2']) }
    let(:client_options)     { {:io_reactor => io_reactor, :registry => cluster_registry} }

    let(:cluster) { Cluster.new(io_reactor, control_connection, cluster_registry, client_options) }

    describe('#hosts') do
      it 'uses State#hosts' do
        expect(cluster.hosts).to eq(cluster_registry.hosts)
      end
    end

    describe('#connect_async') do
      it 'creates a new session' do
        session = cluster.connect_async.get
        expect(cluster_registry).to have(1).listeners
        expect(session).to be_a(Session)
      end

      it 'removes client on close' do
        cluster.connect_async
        cluster_registry.listeners.first.close
        expect(cluster_registry).to have(0).listeners
      end

      it 'uses given keyspace' do
        cluster.connect_async('foo')
        expect(cluster_registry.listeners.first.keyspace).to eq('foo')
      end
    end

    describe('#close_async') do
      let(:promise) { double('promise') }

      before do
        expect(promise).to receive(:map).once.with(cluster).and_return(promise)
      end

      it 'closes control connection' do
        expect(control_connection).to receive(:close_async).once.and_return(promise)
        expect(cluster.close_async).to eq(promise)
      end
    end

    [
      [:connect, ['foo']],
      [:close,   []]
    ].each do |method, args|
      describe("##{method}") do
        let(:promise) { double('promise') }
        let(:result)  { double('result')  }

        before do
          cluster.stub(:"#{method}_async") { promise }
        end

        it "resolves a promise returned by ##{method}_async" do
          expect(promise).to receive(:get).once.and_return(result)
          expect(cluster.__send__(method, *args)).to eq(result)
        end
      end
    end
  end
end
