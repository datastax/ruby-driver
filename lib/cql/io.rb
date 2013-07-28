# encoding: utf-8

module Cql
  IoError = Class.new(CqlError)
  CancelledError = Class.new(CqlError)

  module Io
    ConnectionError = Class.new(IoError)
    ConnectionClosedError = Class.new(ConnectionError)
    ConnectionTimeoutError = Class.new(ConnectionError)
  end
end

require 'cql/io/io_reactor'
require 'cql/io/connection'
