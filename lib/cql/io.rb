# encoding: utf-8

module Cql
  IoError = Class.new(CqlError)

  module Io
    ConnectionError = Class.new(IoError)
    NotRunningError = Class.new(CqlError)
    ConnectionNotFoundError = Class.new(CqlError)
    ConnectionBusyError = Class.new(CqlError)
  end
end

require 'cql/io/io_reactor'
