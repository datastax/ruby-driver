# encoding: utf-8

require 'spec_helper'


module Cql
  module Io
    describe IoReactor do
      let :reactor do
        described_class.new(protocol_handler_factory, selector: selector, clock: clock)
      end

      let :protocol_handler_factory do
        double(:protocol_handler_factory)
      end

      let! :selector do
        IoReactorSpec::FakeSelector.new
      end

      let :clock do
        double(:clock, now: 0)
      end

      describe '#start' do
        after do
          reactor.stop.value if reactor.running?
        end

        it 'returns a future that is resolved when the reactor has started' do
          reactor.start.value
        end

        it 'returns a future that resolves to the reactor' do
          reactor.start.value.should equal(reactor)
        end

        it 'is running after being started' do
          reactor.start.value
          reactor.should be_running
        end

        it 'cannot be started again once stopped' do
          reactor.start.value
          reactor.stop.value
          expect { reactor.start }.to raise_error(ReactorError)
        end

        it 'calls the selector' do
          called = false
          selector.handler { called = true; [[], [], []] }
          reactor.start.value
          await { called }
          reactor.stop.value
          called.should be_true, 'expected the selector to have been called'
        end
      end

      describe '#stop' do
        after do
          reactor.stop.value if reactor.running?
        end

        it 'returns a future that is resolved when the reactor has stopped' do
          reactor.start.value
          reactor.stop.value
        end

        it 'returns a future which resolves to the reactor' do
          reactor.start.value
          reactor.stop.value.should equal(reactor)
        end

        it 'is not running after being stopped' do
          reactor.start.value
          reactor.stop.value
          reactor.should_not be_running
        end

        it 'closes all sockets' do
          connection = nil
          protocol_handler_factory.stub(:new) do |sh, _|
            connection = sh
            double(:protocol_handler)
          end
          reactor.start.value
          reactor.connect('example.com', 9999, 5)
          reactor.stop.value
          connection.should be_closed
        end

        it 'cancels all active timers' do
          reactor.start.value
          clock.stub(:now).and_return(1)
          expired_timer = reactor.schedule_timer(1)
          active_timer1 = reactor.schedule_timer(999)
          active_timer2 = reactor.schedule_timer(111)
          expired_timer.should_not_receive(:fail)
          clock.stub(:now).and_return(2)
          await { expired_timer.completed? }
          reactor.stop.value
          active_timer1.should be_failed
          active_timer2.should be_failed
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
          reactor.start.value
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
          double(:protocol_handler)
        end

        before do
          protocol_handler_factory.stub(:new) do |connection, _|
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

        it 'calls #new on the protocol handler factory with the connection and the reactor itself' do
          reactor.start.value
          reactor.connect('example.com', 9999, 5).value
          protocol_handler_factory.should have_received(:new).with(an_instance_of(Connection), reactor)
        end

        it 'returns a future that resolves to a new protocol handler' do
          reactor.start.value
          f = reactor.connect('example.com', 9999, 5)
          f.value.should equal(protocol_handler)
        end

        it 'returns a new protocol handler which wraps a socket handler' do
          reactor.start.value
          protocol_handler = reactor.connect('example.com', 9999, 5).value
          protocol_handler.connection.should_not be_nil
          protocol_handler.connection.host.should == 'example.com'
          protocol_handler.connection.port.should == 9999
          protocol_handler.connection.connection_timeout.should == 5
        end
      end

      describe '#schedule_timer' do
        before do
          reactor.start.value
        end

        after do
          reactor.stop.value
        end

        it 'returns a future that is resolved after the specified duration' do
          clock.stub(:now).and_return(1)
          f = reactor.schedule_timer(0.1)
          clock.stub(:now).and_return(1.1)
          await { f.resolved? }
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
        described_class.new(selector: selector, clock: clock)
      end

      let :selector do
        double(:selector)
      end

      let :clock do
        double(:clock, now: 0)
      end

      let :socket do
        double(:socket, connected?: false, connecting?: false, writable?: false, closed?: false)
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

        it 'completes timers that have expired' do
          selector.stub(:select).and_return([nil, nil, nil])
          clock.stub(:now).and_return(1)
          promise = Promise.new
          loop_body.schedule_timer(1, promise)
          loop_body.tick
          promise.future.should_not be_completed
          clock.stub(:now).and_return(2)
          loop_body.tick
          promise.future.should be_completed
        end

        it 'clears out timers that have expired' do
          selector.stub(:select).and_return([nil, nil, nil])
          clock.stub(:now).and_return(1)
          promise = Promise.new
          loop_body.schedule_timer(1, promise)
          clock.stub(:now).and_return(2)
          loop_body.tick
          promise.future.should be_completed
          promise.should_not_receive(:fulfill)
          loop_body.tick
        end
      end

      describe '#close_sockets' do
        it 'closes all sockets' do
          socket1 = double(:socket1, closed?: false)
          socket2 = double(:socket2, closed?: false)
          socket1.should_receive(:close)
          socket2.should_receive(:close)
          loop_body.add_socket(socket1)
          loop_body.add_socket(socket2)
          loop_body.close_sockets
        end

        it 'closes all sockets, even when one of them raises an error' do
          socket1 = double(:socket1, closed?: false)
          socket2 = double(:socket2, closed?: false)
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

      describe '#cancel_timers' do
        before do
          selector.stub(:select).and_return([nil, nil, nil])
        end

        it 'fails all active timers with a CancelledError' do
          p1 = Promise.new
          p2 = Promise.new
          p3 = Promise.new
          clock.stub(:now).and_return(1)
          loop_body.schedule_timer(1, p1)
          loop_body.schedule_timer(3, p2)
          loop_body.schedule_timer(3, p3)
          clock.stub(:now).and_return(2)
          loop_body.tick
          loop_body.cancel_timers
          p1.future.should be_completed
          p2.future.should be_failed
          p3.future.should be_failed
          expect { p3.future.value }.to raise_error(CancelledError)
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