# encoding: utf-8

require 'spec_helper'


module Cql
  module Io
    describe IoReactor do
      let :reactor do
        described_class.new(protocol_handler_factory, selector: selector)
      end

      let :protocol_handler_factory do
        stub(:protocol_handler_factory)
      end

      let! :selector do
        IoReactorSpec::FakeSelector.new
      end

      describe '#start' do
        after do
          reactor.stop.get if reactor.running?
        end

        it 'returns a future that completes when the reactor has started' do
          reactor.start.get
        end

        it 'returns a future that resolves to the reactor' do
          reactor.start.get.should equal(reactor)
        end

        it 'is running after being started' do
          reactor.start.get
          reactor.should be_running
        end

        it 'cannot be started again once stopped' do
          reactor.start.get
          reactor.stop.get
          expect { reactor.start }.to raise_error(ReactorError)
        end

        it 'calls the selector' do
          called = false
          selector.handler { called = true; [[], [], []] }
          reactor.start.get
          await { called }
          reactor.stop.get
          called.should be_true, 'expected the selector to have been called'
        end
      end

      describe '#stop' do
        after do
          reactor.stop.get if reactor.running?
        end

        it 'returns a future which completes when the reactor has stopped' do
          reactor.start.get
          reactor.stop.get
        end

        it 'returns a future which resolves to the reactor' do
          reactor.start.get
          reactor.stop.get.should equal(reactor)
        end

        it 'is not running after being stopped' do
          reactor.start.get
          reactor.stop.get
          reactor.should_not be_running
        end

        it 'closes all sockets' do
          connection = nil
          protocol_handler_factory.stub(:new) do |sh|
            connection = sh
            stub(:protocol_handler)
          end
          reactor.start.get
          reactor.connect('example.com', 9999, 5)
          reactor.stop.get
          connection.should be_closed
        end
      end

      describe '#on_error' do
        before do
          selector.handler { raise 'Blurgh' }
        end

        it 'calls the listeners when the reactor crashes' do
          error = nil
          reactor.on_error { |e| error = e }
          reactor.start
          await { error }
          error.message.should == 'Blurgh'
        end

        it 'calls the listener immediately when the reactor has already crashed' do
          error = nil
          reactor.start.get
          await { !reactor.running? }
          reactor.on_error { |e| error = e }
          await { error }
        end

        it 'ignores errors raised by listeners' do
          called = false
          reactor.on_error { raise 'Blurgh' }
          reactor.on_error { called = true }
          reactor.start
          await { called }
          called.should be_true, 'expected all close listeners to have been called'
        end
      end

      describe '#connect' do
        let :protocol_handler do
          stub(:protocol_handler)
        end

        before do
          protocol_handler_factory.stub(:new) do |connection|
            connection.to_io.stub(:connect_nonblock)
            protocol_handler.stub(:connection).and_return(connection)
            protocol_handler
          end
        end

        before do
          selector.handler do |readables, writables, _, _|
            writables.each do |writable|
              fake_connected(writable)
            end
            [[], writables, []]
          end
        end

        def fake_connected(connection)
          connection.to_io.stub(:connect_nonblock)
        end

        after do
          reactor.stop if reactor.running?
        end

        it 'returns a future that resolves to a new protocol handler' do
          reactor.start.get
          f = reactor.connect('example.com', 9999, 5)
          f.get.should equal(protocol_handler)
        end

        it 'returns a new protocol handler which wraps a socket handler' do
          reactor.start.get
          protocol_handler = reactor.connect('example.com', 9999, 5).get
          protocol_handler.connection.should_not be_nil
          protocol_handler.connection.host.should == 'example.com'
          protocol_handler.connection.port.should == 9999
          protocol_handler.connection.connection_timeout.should == 5
        end
      end

      describe '#to_s' do
        context 'returns a string that' do
          it 'includes the class name' do
            reactor.to_s.should include('Cql::Io::IoReactor')
          end

          it 'includes a list of its connections' do
            reactor.to_s.should include('@connections=[')
            reactor.to_s.should include('#<Cql::Io::Unblocker>')
          end
        end
      end
    end

    describe IoLoopBody do
      let :loop_body do
        described_class.new(selector: selector)
      end

      let :selector do
        stub(:selector)
      end

      let :socket do
        stub(:socket, connected?: false, connecting?: false, writable?: false, closed?: false)
      end

      describe '#tick' do
        before do
          loop_body.add_socket(socket)
        end

        it 'passes connected sockets as readables to the selector' do
          socket.stub(:connected?).and_return(true)
          selector.should_receive(:select).with([socket], anything, anything, anything).and_return([nil, nil, nil])
          loop_body.tick
        end

        it 'passes writable sockets as writable to the selector' do
          socket.stub(:writable?).and_return(true)
          selector.should_receive(:select).with(anything, [socket], anything, anything).and_return([nil, nil, nil])
          loop_body.tick
        end

        it 'passes connecting sockets as writable to the selector' do
          socket.stub(:connecting?).and_return(true)
          socket.stub(:connect)
          selector.should_receive(:select).with(anything, [socket], anything, anything).and_return([nil, nil, nil])
          loop_body.tick
        end

        it 'filters out closed sockets' do
          socket.stub(:closed?).and_return(true)
          selector.should_receive(:select).with([], [], anything, anything).and_return([nil, nil, nil])
          loop_body.tick
          socket.stub(:connected?).and_return(true)
          selector.should_receive(:select).with([], [], anything, anything).and_return([nil, nil, nil])
          loop_body.tick
        end

        it 'calls #read on all readable sockets returned by the selector' do
          socket.stub(:connected?).and_return(true)
          socket.should_receive(:read)
          selector.stub(:select) do |r, w, _, _|
            [[socket], nil, nil]
          end
          loop_body.tick
        end

        it 'calls #connect on all connecting sockets' do
          socket.stub(:connecting?).and_return(true)
          socket.should_receive(:connect)
          selector.stub(:select).and_return([nil, nil, nil])
          loop_body.tick
        end

        it 'calls #flush on all writable sockets returned by the selector' do
          socket.stub(:writable?).and_return(true)
          socket.should_receive(:flush)
          selector.stub(:select) do |r, w, _, _|
            [nil, [socket], nil]
          end
          loop_body.tick
        end

        it 'allows the caller to specify a custom timeout' do
          selector.should_receive(:select).with(anything, anything, anything, 99).and_return([[], [], []])
          loop_body.tick(99)
        end
      end

      describe '#close_sockets' do
        it 'closes all sockets' do
          socket1 = stub(:socket1, closed?: false)
          socket2 = stub(:socket2, closed?: false)
          socket1.should_receive(:close)
          socket2.should_receive(:close)
          loop_body.add_socket(socket1)
          loop_body.add_socket(socket2)
          loop_body.close_sockets
        end

        it 'closes all sockets, even when one of them raises an error' do
          socket1 = stub(:socket1, closed?: false)
          socket2 = stub(:socket2, closed?: false)
          socket1.stub(:close).and_raise('Blurgh')
          socket2.should_receive(:close)
          loop_body.add_socket(socket1)
          loop_body.add_socket(socket2)
          loop_body.close_sockets
        end

        it 'does not close already closed sockets' do
          socket.stub(:closed?).and_return(true)
          socket.should_not_receive(:close)
          loop_body.add_socket(socket)
          loop_body.close_sockets
        end
      end
    end
  end
end

module IoReactorSpec
  class FakeSelector
    def initialize
      handler { [[], [], []] }
    end

    def handler(&body)
      @body = body
    end

    def select(*args)
      @body.call(*args)
    end
  end
end