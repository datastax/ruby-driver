# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe ClusterConnectionSequence do
      let :cluster_connector do
        described_class.new(node_connector, logger)
      end

      let :node_connector do
        double(:node_connector)
      end

      let :logger do
        NullLogger.new
      end

      def make_connection(host)
        c = double(:connection)
        c.stub(:[]) do |key|
          case key
          when :data_center then 'dc'
          when :host_id then Uuid.new('11111111-1111-1111-1111-111111111111')
          else nil
          end
        end
        c.stub(:host).and_return(host)
        c.stub(:port).and_return(9999)
        c.stub(:on_closed) do |&listener|
          c.stub(:closed_listener).and_return(listener)
        end
        c.stub(:connected?).and_return(true)
        c
      end

      describe '#connect_all' do
        let :connections do
          []
        end

        let :bad_nodes do
          []
        end

        let :failure do
          [StandardError.new('bork')]
        end

        before do
          node_connector.stub(:connect) do |host|
            connections << host
            if bad_nodes.include?(host)
              Future.failed(failure[0])
            else
              Future.resolved(make_connection(host))
            end
          end
        end

        it 'connects to each host' do
          f = cluster_connector.connect_all(%w[host0 host1], 1)
          f.value
          connections.should include('host0', 'host1')
        end

        it 'connects multiple times to each node' do
          f = cluster_connector.connect_all(%w[host0 host1], 3)
          f.value
          connections.count { |c| c == 'host0' }.should == 3
          connections.count { |c| c == 'host1' }.should == 3
        end

        it 'returns a future that resolves to the connections' do
          f = cluster_connector.connect_all(%w[host0 host1], 2)
          f.value.map(&:host).sort.should == %w[host0 host0 host1 host1]
        end

        it 'succeeds as long as one connection succeeds' do
          bad_nodes.push('host0')
          f = cluster_connector.connect_all(%w[host0 host1], 1)
          f.value.map(&:host).sort.should == %w[host1]
        end

        it 'fails when all connections fail' do
          bad_nodes.push('host0')
          bad_nodes.push('host1')
          f = cluster_connector.connect_all(%w[host0 host1], 1)
          expect { f.value }.to raise_error
        end

        it 'fails with an AuthenticationError when the connections failed with a QueryError with error code 0x100' do
          bad_nodes.push('host0')
          bad_nodes.push('host1')
          failure[0] = QueryError.new(0x100, 'bork')
          f = cluster_connector.connect_all(%w[host0 host1], 1)
          expect { f.value }.to raise_error(AuthenticationError)
        end

        it 'logs when a connection is complete' do
          logger.stub(:info)
          f = cluster_connector.connect_all(%w[host0 host1], 1)
          f.value
          logger.should have_received(:info).with(/connected to node .{36} at host0:9999 in data center dc/i)
          logger.should have_received(:info).with(/connected to node .{36} at host1:9999 in data center dc/i)
        end

        it 'logs when a connection fails' do
          logger.stub(:warn)
          bad_nodes.push('host0')
          f = cluster_connector.connect_all(%w[host0 host1], 1)
          f.value
          logger.should have_received(:warn).with(/failed connecting to node at host0: bork/i)
        end

        it 'registers a listener that logs when a connection closes' do
          logger.stub(:info)
          f = cluster_connector.connect_all(%w[host0 host1], 1)
          connection = f.value.first
          connection.closed_listener.call
          logger.should have_received(:info).with(/connection to node .{36} at host0:9999 in data center dc closed/i)
        end

        it 'registers a listener that logs when a connection closes unexpectedly' do
          logger.stub(:warn)
          f = cluster_connector.connect_all(%w[host0 host1], 1)
          connection = f.value.first
          connection.closed_listener.call(StandardError.new('BORK'))
          logger.should have_received(:warn).with(/connection to node .{36} at host0:9999 in data center dc closed unexpectedly: BORK/i)
        end
      end
    end

    describe ConnectionSequence do
      let :sequence do
        described_class.new(steps)
      end

      let :steps do
        [
          double(:step0),
          double(:step1),
          double(:step2),
        ]
      end

      describe '#connect' do
        it 'calls the first step with an object that has the connection parameters' do
          steps[0].stub(:run) do |arg|
            steps[0].stub(:arg).and_return(arg)
            Future.resolved(arg)
          end
          sequence = described_class.new(steps.take(1))
          result = sequence.connect('host0')
          steps[0].arg.host.should == 'host0'
        end

        it 'expects the last step to return a future that resolves to an object that has a connection' do
          steps[0].stub(:run) do |arg|
            Future.resolved(double(connection: :fake_connection))
          end
          sequence = described_class.new(steps.take(1))
          result = sequence.connect('host0')
          result.value.should == :fake_connection
        end

        it 'passes the result of a step as argument to the next step' do
          steps[0].stub(:run) do |arg|
            Future.resolved(:foo)
          end
          steps[1].stub(:run) do |arg|
            steps[1].stub(:arg).and_return(arg)
            Future.resolved(:bar)
          end
          steps[2].stub(:run) do |arg|
            steps[2].stub(:arg).and_return(arg)
            Future.resolved(double(connection: :fake_connection))
          end
          sequence = described_class.new(steps)
          result = sequence.connect('host0')
          steps[1].arg.should == :foo
          steps[2].arg.should == :bar
        end
      end
    end
  end
end