# encoding: utf-8

module Cql
  IoError = Class.new(CqlError)

  module Io
    ConnectionError = Class.new(IoError)
    ConnectionTimeoutError = Class.new(ConnectionError)
  end
end

require 'cql/io/io_reactor'
require 'cql/io/connection'
