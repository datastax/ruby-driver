# encoding: utf-8

require 'spec_helper'


module Cql
  module Io
    describe Connection do
      let :handler do
        described_class.new('example.com', 55555, 5, unblocker, socket_impl, clock)
      end

      let :unblocker do
        stub(:unblocker, unblock!: nil)
      end

      let :socket_impl do
        stub(:socket_impl)
      end

      let :clock do
        stub(:clock, now: 0)
      end

      let :socket do
        stub(:socket)
      end

      before do
        socket_impl.stub(:getaddrinfo)
          .with('example.com', 55555, nil, Socket::SOCK_STREAM)
          .and_return([[nil, 'PORT', nil, 'IP1', 'FAMILY1', 'TYPE1'], [nil, 'PORT', nil, 'IP2', 'FAMILY2', 'TYPE2']])
        socket_impl.stub(:sockaddr_in)
          .with('PORT', 'IP1')
          .and_return('SOCKADDR1')
        socket_impl.stub(:new)
          .with('FAMILY1', 'TYPE1', 0)
          .and_return(socket)
      end

      describe '#connect' do
        it 'creates a socket and calls #connect_nonblock' do
          socket.should_receive(:connect_nonblock).with('SOCKADDR1')
          handler.connect
        end

        it 'handles EINPROGRESS that #connect_nonblock raises' do
          socket.stub(:connect_nonblock).and_raise(Errno::EINPROGRESS)
          handler.connect
        end

        it 'is connecting after #connect has been called' do
          socket.stub(:connect_nonblock).and_raise(Errno::EINPROGRESS)
          handler.connect
          handler.should be_connecting
        end

        it 'is connecting even after the second call' do
          socket.should_receive(:connect_nonblock).twice.and_raise(Errno::EINPROGRESS)
          handler.connect
          handler.connect
          handler.should be_connecting
        end

        it 'does not create a new socket the second time' do
          socket_impl.should_receive(:new).once.and_return(socket)
          socket.stub(:connect_nonblock).and_raise(Errno::EINPROGRESS)
          handler.connect
          handler.connect
        end

        it 'attempts another connect the second time' do
          socket.should_receive(:connect_nonblock).twice.and_raise(Errno::EINPROGRESS)
          handler.connect
          handler.connect
        end

        shared_examples 'on successfull connection' do
          it 'completes the returned future and returns itself' do
            f = handler.connect
            f.should be_complete
            f.get.should equal(handler)
          end

          it 'is connected' do
            handler.connect
            handler.should be_connected
          end
        end

        context 'when #connect_nonblock does not raise any error' do
          before do
            socket.stub(:connect_nonblock)
          end

          include_examples 'on successfull connection'
        end

        context 'when #connect_nonblock raises EISCONN' do
          before do
            socket.stub(:connect_nonblock).and_raise(Errno::EISCONN)
          end

          include_examples 'on successfull connection'
        end

        context 'when #connect_nonblock raises EALREADY' do
          it 'it does nothing' do
            socket.stub(:connect_nonblock).and_raise(Errno::EALREADY)
            f = handler.connect
            f.should_not be_complete
            f.should_not be_failed
          end
        end

        context 'when #connect_nonblock raises EINVAL' do
          before do
            socket_impl.stub(:sockaddr_in)
              .with('PORT', 'IP2')
              .and_return('SOCKADDR2')
            socket_impl.stub(:new)
              .with('FAMILY2', 'TYPE2', 0)
              .and_return(socket)
            socket.stub(:close)
          end

          it 'attempts to connect to the next address given by #getaddinfo' do
            socket.should_receive(:connect_nonblock).with('SOCKADDR1').and_raise(Errno::EINVAL)
            socket.should_receive(:connect_nonblock).with('SOCKADDR2')
            handler.connect
          end

          it 'fails if there are no more addresses to try' do
            socket.stub(:connect_nonblock).and_raise(Errno::EINVAL)
            f = handler.connect
            expect { f.get }.to raise_error(ConnectionError)
          end
        end

        context 'when #connect_nonblock raises SystemCallError' do
          before do
            socket.stub(:connect_nonblock).and_raise(SystemCallError.new('Bork!', 9999))
            socket.stub(:close)
          end

          it 'fails the future with a ConnectionError' do
            f = handler.connect
            expect { f.get }.to raise_error(ConnectionError)
          end

          it 'closes the socket' do
            socket.should_receive(:close)
            handler.connect
          end

          it 'calls the closed listener' do
            called = false
            handler.on_closed { called = true }
            handler.connect
            called.should be_true, 'expected the close listener to have been called'
          end

          it 'passes the error to the close listener' do
            error = nil
            handler.on_closed { |e| error = e }
            handler.connect
            error.should be_a(Exception)
          end

          it 'is closed' do
            handler.connect
            handler.should be_closed
          end
        end

        context 'when Socket.getaddrinfo raises SocketError' do
          before do
            socket_impl.stub(:getaddrinfo).and_raise(SocketError)
          end

          it 'fails the returned future with a ConnectionError' do
            f = handler.connect
            expect { f.get }.to raise_error(ConnectionError)
          end

          it 'calls the close listener' do
            called = false
            handler.on_closed { called = true }
            handler.connect
            called.should be_true, 'expected the close listener to have been called'
          end

          it 'passes the error to the close listener' do
            error = nil
            handler.on_closed { |e| error = e }
            handler.connect
            error.should be_a(Exception)
          end

          it 'is closed' do
            handler.connect
            handler.should be_closed
          end
        end

        context 'when it takes longer than the connection timeout to connect' do
          before do
            socket.stub(:connect_nonblock).and_raise(Errno::EINPROGRESS)
            socket.stub(:close)
          end

          it 'fails the returned future with a ConnectionTimeoutError' do
            f = handler.connect
            clock.stub(:now).and_return(1)
            handler.connect
            socket.should_receive(:close)
            clock.stub(:now).and_return(7)
            handler.connect
            f.should be_failed
            expect { f.get }.to raise_error(ConnectionTimeoutError)
          end

          it 'closes the connection' do
            handler.connect
            clock.stub(:now).and_return(1)
            handler.connect
            socket.should_receive(:close)
            clock.stub(:now).and_return(7)
            handler.connect
          end

          it 'delivers a ConnectionTimeoutError to the close handler' do
            error = nil
            handler.on_closed { |e| error = e }
            handler.connect
            clock.stub(:now).and_return(7)
            handler.connect
            error.should be_a(ConnectionTimeoutError)
          end
        end
      end

      describe '#close' do
        before do
          socket.stub(:connect_nonblock)
          socket.stub(:close)
          handler.connect
        end

        it 'closes the socket' do
          socket.should_receive(:close)
          handler.close
        end

        it 'returns true' do
          handler.close.should be_true
        end

        it 'swallows SystemCallErrors' do
          socket.stub(:close).and_raise(SystemCallError.new('Bork!', 9999))
          handler.close
        end

        it 'swallows IOErrors' do
          socket.stub(:close).and_raise(IOError.new('Bork!'))
          handler.close
        end

        it 'calls the closed listener' do
          called = false
          handler.on_closed { called = true }
          handler.close
          called.should be_true, 'expected the close listener to have been called'
        end

        it 'does nothing when closed a second time' do
          socket.should_receive(:close).once
          calls = 0
          handler.on_closed { calls += 1 }
          handler.close
          handler.close
          calls.should == 1
        end

        it 'returns false if it did nothing' do
          handler.close
          handler.close.should be_false
        end

        it 'is not writable when closed' do
          handler.write('foo')
          handler.close
          handler.should_not be_writable
        end
      end

      describe '#to_io' do
        before do
          socket.stub(:connect_nonblock)
          socket.stub(:close)
        end

        it 'returns nil initially' do
          handler.to_io.should be_nil
        end

        it 'returns the socket when connected' do
          handler.connect
          handler.to_io.should equal(socket)
        end

        it 'returns nil when closed' do
          handler.connect
          handler.close
          handler.to_io.should be_nil
        end
      end

      describe '#write/#flush' do
        before do
          socket.stub(:connect_nonblock)
          handler.connect
        end

        it 'appends to its buffer when #write is called' do
          handler.write('hello world')
        end

        it 'unblocks the reactor' do
          unblocker.should_receive(:unblock!)
          handler.write('hello world')
        end

        it 'is writable when there are bytes to write' do
          handler.should_not be_writable
          handler.write('hello world')
          handler.should be_writable
          socket.should_receive(:write_nonblock).with('hello world').and_return(11)
          handler.flush
          handler.should_not be_writable
        end

        it 'writes to the socket from its buffer when #flush is called' do
          handler.write('hello world')
          socket.should_receive(:write_nonblock).with('hello world').and_return(11)
          handler.flush
        end

        it 'takes note of how much the #write_nonblock call consumed and writes the rest of the buffer on the next call to #flush' do
          handler.write('hello world')
          socket.should_receive(:write_nonblock).with('hello world').and_return(6)
          handler.flush
          socket.should_receive(:write_nonblock).with('world').and_return(5)
          handler.flush
        end

        it 'does not call #write_nonblock if the buffer is empty' do
          handler.flush
          handler.write('hello world')
          socket.should_receive(:write_nonblock).with('hello world').and_return(11)
          handler.flush
          socket.should_not_receive(:write_nonblock)
          handler.flush
        end

        context 'with a block' do
          it 'yields a byte buffer to the block' do
            socket.should_receive(:write_nonblock).with('hello world').and_return(11)
            handler.write do |buffer|
              buffer << 'hello world'
            end
            handler.flush
          end
        end

        context 'when #write_nonblock raises an error' do
          before do
            socket.stub(:close)
            socket.stub(:write_nonblock).and_raise('Bork!')
          end

          it 'closes the socket' do
            socket.should_receive(:close)
            handler.write('hello world')
            handler.flush
          end

          it 'passes the error to the close handler' do
            error = nil
            handler.on_closed { |e| error = e }
            handler.write('hello world')
            handler.flush
            error.should be_a(Exception)
          end
        end
      end

      describe '#read' do
        before do
          socket.stub(:connect_nonblock)
          handler.connect
        end

        it 'reads a chunk from the socket' do
          socket.should_receive(:read_nonblock).with(instance_of(Fixnum)).and_return('foo bar')
          handler.read
        end

        it 'calls the data listener with the new data' do
          socket.should_receive(:read_nonblock).with(instance_of(Fixnum)).and_return('foo bar')
          data = nil
          handler.on_data { |d| data = d }
          handler.read
          data.should == 'foo bar'
        end

        context 'when #read_nonblock raises an error' do
          before do
            socket.stub(:close)
            socket.stub(:read_nonblock).and_raise('Bork!')
          end

          it 'closes the socket' do
            socket.should_receive(:close)
            handler.read
          end

          it 'passes the error to the close handler' do
            error = nil
            handler.on_closed { |e| error = e }
            handler.read
            error.should be_a(Exception)
          end
        end
      end

      describe '#to_s' do
        context 'returns a string that' do
          it 'includes the class name' do
            handler.to_s.should include('Cql::Io::Connection')
          end

          it 'includes the host and port' do
            handler.to_s.should include('example.com:55555')
          end

          it 'includes the connection state' do
            handler.to_s.should include('closed')
            socket.stub(:connect_nonblock).and_raise(Errno::EINPROGRESS)
            handler.connect
            handler.to_s.should include('connecting')
            socket.stub(:connect_nonblock)
            handler.connect
            handler.to_s.should include('connected')
          end
        end
      end
    end
  end
end
