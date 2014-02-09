# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe ClusterConnector do
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

    describe Connector do
      let :connector do
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
          connector = described_class.new(steps.take(1))
          result = connector.connect('host0')
          steps[0].arg.host.should == 'host0'
        end

        it 'expects the last step to return a future that resolves to an object that has a connection' do
          steps[0].stub(:run) do |arg|
            Future.resolved(double(connection: :fake_connection))
          end
          connector = described_class.new(steps.take(1))
          result = connector.connect('host0')
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
          connector = described_class.new(steps)
          result = connector.connect('host0')
          steps[1].arg.should == :foo
          steps[2].arg.should == :bar
        end

        it 'fails if any of the steps fail' do
          steps[0].stub(:run) do |arg|
            Future.resolved(:foo)
          end
          steps[1].stub(:run) do |arg|
            raise 'bork'
          end
          steps[2].stub(:run) do |arg|
            Future.resolved(double(connection: :fake_connection))
          end
          connector = described_class.new(steps)
          result = connector.connect('host0')
          expect { result.value }.to raise_error('bork')
          steps[2].should_not have_received(:run)
        end
      end
    end

    describe ConnectStep do
      let :step do
        described_class.new(io_reactor, 1111, 9, logger)
      end

      let :pending_connection do
        double(:pending_connection)
      end

      let :new_pending_connection do
        double(:new_pending_connection)
      end

      let :io_reactor do
        double(:io_reactor)
      end

      let :logger do
        NullLogger.new
      end

      let :connection do
        double(:connection)
      end

      describe '#run' do
        before do
          pending_connection.stub(:host).and_return('example.com')
          pending_connection.stub(:with_connection).and_return(new_pending_connection)
          io_reactor.stub(:connect).and_return(Future.resolved(connection))
        end

        it 'connects using the connection details given' do
          step.run(pending_connection)
          io_reactor.should have_received(:connect).with('example.com', 1111, 9)
        end

        it 'appends the connection to the given argument and returns the result' do
          result = step.run(pending_connection)
          result.value.should equal(new_pending_connection)
        end

        it 'logs a message when it starts connecting' do
          logger.stub(:debug)
          step.run(pending_connection)
          logger.should have_received(:debug).with(/connecting to node at example\.com:1111/i)
        end

        it 'returns a failed future when the connection fails' do
          io_reactor.stub(:connect).and_return(Future.failed(StandardError.new('bork')))
          result = step.run(pending_connection)
          expect { result.value }.to raise_error('bork')
        end
      end
    end

    describe CacheOptionsStep do
      let :step do
        described_class.new
      end

      let :pending_connection do
        double(:pending_connection)
      end

      describe '#run' do
        before do
          response = {'CQL_VERSION' => %w[3.1.2], 'COMPRESSION' => %w[snappy magic]}
          pending_connection.stub(:execute).with(an_instance_of(Protocol::OptionsRequest)).and_return(Future.resolved(response))
          pending_connection.stub(:[]=)
        end

        it 'runs an OPTIONS request and adds the CQL version and compression support to the connection metadata' do
          step.run(pending_connection)
          pending_connection.should have_received(:[]=).with(:cql_version, %w[3.1.2])
          pending_connection.should have_received(:[]=).with(:compression, %w[snappy magic])
        end

        it 'returns the same argument as it was given' do
          step.run(pending_connection).value.should equal(pending_connection)
        end

        it 'returns a failed future when the request fails' do
          pending_connection.stub(:execute).and_return(Future.failed(StandardError.new('bork')))
          result = step.run(pending_connection)
          expect { result.value }.to raise_error('bork')
        end
      end
    end

    describe InitializeStep do
      let :step do
        described_class.new(compressor, logger)
      end

      let :compressor do
        nil
      end

      let :logger do
        NullLogger.new
      end

      let :pending_connection do
        double(:pending_connection)
      end

      describe '#run' do
        before do
          pending_connection.stub(:[]).with(:compression).and_return(%w[magic snappy])
          pending_connection.stub(:execute) do |request|
            pending_connection.stub(:last_request).and_return(request)
            Future.resolved
          end
        end

        it 'sends a STARTUP request' do
          step.run(pending_connection)
          pending_connection.last_request.should be_a(Protocol::StartupRequest)
        end

        it 'does not set the compression option when there is no compressor' do
          step.run(pending_connection)
          pending_connection.last_request.options.should_not have_key('COMPRESSION')
        end

        it 'returns the same argument as it was given' do
          step.run(pending_connection).value.should equal(pending_connection)
        end

        it 'returns a failed future when the request fails' do
          pending_connection.stub(:execute).and_return(Future.failed(StandardError.new('bork')))
          result = step.run(pending_connection)
          expect { result.value }.to raise_error('bork')
        end

        context 'when a compressor is given' do
          let :compressor do
            double(:compressor, algorithm: 'magic')
          end

          it 'sets the compression option to the algorithm supported by the compressor' do
            step.run(pending_connection)
            pending_connection.last_request.options.should include('COMPRESSION' => 'magic')
          end

          it 'does not set the compression option when the algorithm is not supported by the server' do
            compressor.stub(:algorithm).and_return('bogus')
            step.run(pending_connection)
            pending_connection.last_request.options.should_not have_key('COMPRESSION')
          end

          it 'logs a message when the server supports the algorithm' do
            logger.stub(:debug)
            step.run(pending_connection)
            logger.should have_received(:debug).with(/using "magic" compression/i)
          end

          it 'logs a message when the algorithm is not supported by the server' do
            pending_connection.stub(:[]).with(:compression).and_return(%w[foo bar])
            logger.stub(:warn)
            step.run(pending_connection)
            logger.should have_received(:warn).with(/algorithm "magic" not supported/i)
          end

          it 'logs which algorithms the server supports when there is a mismatch' do
            pending_connection.stub(:[]).with(:compression).and_return(%w[foo bar])
            logger.stub(:warn)
            step.run(pending_connection)
            logger.should have_received(:warn).with(/server supports "foo", "bar"/i)
          end
        end

        context 'when authentication is required' do
          let :new_pending_connection do
            double(:new_pending_connection)
          end

          before do
            pending_connection.stub(:execute).and_return(Future.resolved(AuthenticationRequired.new('net.acme.Bogus')))
            pending_connection.stub(:with_authentication_class).and_return(new_pending_connection)
          end

          it 'appends the authentication class returned by the server to given argument and returns the result' do
            step.run(pending_connection).value.should equal(new_pending_connection)
          end
        end
      end
    end

    describe AuthenticationStep do
      let :step do
        described_class.new(authenticator, 5)
      end

      let :authenticator do
        double(:authenticator)
      end

      let :pending_connection do
        double(:pending_connection)
      end

      let :request do
        double(:request)
      end

      describe '#run' do
        before do
          pending_connection.stub(:authentication_class).and_return('org.acme.Auth')
          pending_connection.stub(:execute).and_return(Future.resolved)
          authenticator.stub(:supports?).and_return(true)
          authenticator.stub(:initial_request).and_return(request)
        end

        it 'returns the pending connection when there\'s no authentication class' do
          pending_connection.stub(:authentication_class).and_return(nil)
          result = step.run(pending_connection)
          result.value.should equal(pending_connection)
        end

        it 'returns a failed future when there\'s an authentication class but no authenticator' do
          step = described_class.new(nil, 5)
          result = step.run(pending_connection)
          expect { result.value }.to raise_error(AuthenticationError)
        end

        it 'returns a failed future when the authenticator does not support the authentication class' do
          authenticator.stub(:supports?).and_return(false)
          result = step.run(pending_connection)
          expect { result.value }.to raise_error(AuthenticationError)
        end

        it 'asks the authenticator to formulate its initial requests, and then executes the request' do
          step.run(pending_connection)
          pending_connection.should have_received(:execute).with(request)
        end

        it 'passes the protocol version to the authenticator' do
          step.run(pending_connection)
          authenticator.should have_received(:initial_request).with(5)
        end

        it 'returns the same argument as it was given' do
          result = step.run(pending_connection)
          result.value.should equal(pending_connection)
        end
      end
    end

    describe CachePropertiesStep do
      let :step do
        described_class.new
      end

      let :pending_connection do
        double(:pending_connection)
      end

      describe '#run' do
        before do
          node_info = {'data_center' => 'dc', 'host_id' => Uuid.new('11111111-1111-1111-1111-111111111111')}
          pending_connection.stub(:execute) do |request|
            pending_connection.stub(:last_request).and_return(request)
            Future.resolved(QueryResult.new([], [node_info], nil, nil))
          end
          pending_connection.stub(:[]=)
        end

        it 'queries the system table "local" for data center and host ID and adds these to the connection metadata' do
          step.run(pending_connection)
          pending_connection.should have_received(:[]=).with(:data_center, 'dc')
          pending_connection.should have_received(:[]=).with(:host_id, Uuid.new('11111111-1111-1111-1111-111111111111'))
        end

        it 'handles the case when the query result is empty' do
          pending_connection.stub(:execute).and_return(Future.resolved(QueryResult.new([], [], nil, nil)))
          result = step.run(pending_connection)
          result.should be_resolved
          pending_connection.should_not have_received(:[]=)
        end

        it 'returns the same argument as it was given' do
          step.run(pending_connection).value.should equal(pending_connection)
        end

        it 'returns a failed future when the request fails' do
          pending_connection.stub(:execute).and_return(Future.failed(StandardError.new('bork')))
          result = step.run(pending_connection)
          expect { result.value }.to raise_error('bork')
        end
      end
    end
  end
end