# encoding: utf-8

module Cql
  IoError = Class.new(CqlError)

  module Io
    ConnectionError = Class.new(IoError)
    ConnectionClosedError = Class.new(IoError)
    ConnectionTimeoutError = Class.new(ConnectionError)
    NotRunningError = Class.new(CqlError)
    ConnectionNotFoundError = Class.new(CqlError)
    ConnectionBusyError = Class.new(CqlError)
  end
end

require 'cql/io/io_reactor'
require 'cql/io/socket_handler'
require 'cql/io/cql_connection'
require 'cql/io/node_connection'
