# encoding: utf-8

require 'spec_helper'

class FakeClusterState
  attr_reader :ips, :clients, :hosts

  def initialize(ips)
    @ips     = ips
    @hosts   = Set[ips.map {|ip| Cql::Cluster::Host.new(ip)}]
    @clients = Set.new
  end

  def add_client(client)
    @clients << client
    self
  end

  def remove_client(client)
    @clients.delete(client)
    self
  end

  def has_clients?
    !@clients.empty?
  end

  def each_client(&block)
    @clients.each(&block)
  end
end

module Cql
  describe(Cluster) do
    let(:io_reactor)         { FakeIoReactor.new }
    let(:control_connection) { double('control connection') }
    let(:cluster_state)      { FakeClusterState.new(['1.1.1.1', '2.2.2.2']) }
    let(:client_options)     { {:io_reactor => io_reactor} }

    let(:cluster) { Cluster.new(io_reactor, control_connection, cluster_state, client_options) }

    describe('#hosts') do
      it 'uses State#hosts' do
        expect(cluster.hosts).to eq(cluster_state.hosts)
      end
    end

    describe('#connect_async') do
      it 'creates a new session' do
        session = cluster.connect_async.get
        expect(cluster_state).to have(1).clients
        expect(session).to be_a(Session)
      end

      it 'removes client on close' do
        cluster.connect_async
        cluster_state.clients.first.close
        expect(cluster_state).to have(0).clients
      end

      it 'uses given keyspace' do
        cluster.connect_async('foo')
        expect(cluster_state.clients.first.keyspace).to eq('foo')
      end
    end

    describe('#close_async') do
      let(:promise) { double('promise') }

      before do
        expect(promise).to receive(:map).once.with(cluster).and_return(promise)
      end

      context('without clients') do
        it 'closes control connection' do
          expect(control_connection).to receive(:close_async).once.and_return(promise)
          expect(io_reactor).to receive(:stop).and_call_original
          expect(promise).to receive(:flat_map).and_yield
          expect(cluster.close_async).to eq(promise)
        end
      end

      context('with clients') do
        let(:clients) do
          Array.new(5) { |i| double("client #{i + 1}") }
        end

        let(:client_futures) do
          Array.new(5) { |i| double("client #{i + 1} close future") }
        end

        let(:control_connection_future) { double('control connection close future') }

        before do
          clients.each_with_index do |client, i|
            cluster_state.add_client(client)
            client.stub(:shutdown) { client_futures[i] }
          end

          control_connection.stub(:close_async) { control_connection_future }
        end

        it 'closes all clients and control connection' do
          expect(Cql::Future).to receive(:all).once.with(*client_futures, control_connection_future).and_return(promise)
          expect(promise).to receive(:flat_map).and_return(promise)
          expect(cluster.close_async).to eq(promise)
        end
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
