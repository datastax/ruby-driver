# encoding: utf-8

require 'spec_helper'

module Cql
  describe(Cluster) do
    let(:io_reactor)         { FakeIoReactor.new }
    let(:control_connection) { double('control connection') }
    let(:cluster_registry)   { FakeClusterRegistry.new(['1.1.1.1', '2.2.2.2']) }
    let(:load_balancing_policy) { FakeLoadBalancingPolicy.new(cluster_registry) }
    let(:driver)             { Driver.new({
                                 :io_reactor => io_reactor,
                                 :cluster_registry => cluster_registry,
                                 :control_connection => control_connection,
                                 :load_balancing_policy => load_balancing_policy
                               })
                             }

    let(:cluster) { Cluster.new(driver.logger, io_reactor, control_connection, cluster_registry, driver.execution_options, load_balancing_policy, driver.reconnection_policy, driver.retry_policy, driver.connector) }

    describe('#hosts') do
      it 'uses State#hosts' do
        expect(cluster.hosts).to eq(cluster_registry.hosts)
      end
    end

    describe('#connect_async') do
      let(:client) { double('cluster client') }
      let(:session) { double('session') }

      it 'creates a new session' do
        cluster.connect_async.get
        expect(cluster_registry).to have(1).listeners
      end

      it 'removes client on close' do
        cluster.connect_async
        cluster_registry.listeners.first.close
        expect(cluster_registry).to have(0).listeners
      end

      it 'uses given keyspace' do
        future = Future.resolved
        Session.stub(:new) { session }
        expect(session).to receive(:execute_async).once.with('USE foo').and_return(future)
        cluster.connect_async('foo').get
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
