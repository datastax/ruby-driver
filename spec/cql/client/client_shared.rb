# encoding: utf-8

shared_context 'client setup' do
  let :connection_options do
    {:host => 'example.com', :port => 12321, :io_reactor => io_reactor}
  end

  let :io_reactor do
    FakeIoReactor.new
  end

  def connections
    io_reactor.connections
  end

  def connection
    connections.first
  end

  def requests
    connection[:requests]
  end

  def last_request
    requests.last
  end
end