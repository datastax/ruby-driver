# encoding: utf-8

class FakeServer
  attr_reader :connects, :disconnects

  def initialize(port)
    @port = port
    @state = {}
    @lock = Mutex.new
    @connects = 0
    @disconnects = 0
    @connections = []
    @received_bytes = ''
  end

  def start!
    @lock.synchronize do
      return if @running
      @running = true
    end
    @sockets = [TCPServer.new(@port)]
    @started = Cql::Future.new
    @thread = Thread.start do
      Thread.current.abort_on_exception = true
      @started.complete!
      io_loop
    end
    @started.get
    self
  end

  def stop!
    @lock.synchronize do
      return unless @running
      @running = false
    end
    @thread.join if @thread
    @sockets.each(&:close)
  end

  def broadcast!(bytes)
    @lock.synchronize do
      @connections.each { |c| c.write_nonblock(bytes) }
    end
  end

  def await_connects!(n=1)
    sleep(0.01) until @connects >= n
  end

  def await_disconnects!(n=1)
    sleep(0.01) until @disconnects >= n
  end

  def received_bytes
    @lock.synchronize do
      return @received_bytes.dup
    end
  end

  private

  def io_loop
    while @running
      acceptables, _ = IO.select(@sockets, @connections, nil, 0)
      readables, writables, _ = IO.select(@connections, @connections, nil, 0)

      if acceptables
        acceptables.each do |socket|
          connection, _ = socket.accept_nonblock
          @lock.synchronize do
            @connects += 1
            @connections << connection
          end
        end
      end

      if readables
        readables.each do |readable|
          begin
            bytes = readable.read_nonblock(2**16)
            @lock.synchronize do
              @received_bytes << bytes
            end
          rescue EOFError
            @lock.synchronize do
              @connections.delete(readable)
              @disconnects += 1
            end
          end
        end
      end
    end
  end
end