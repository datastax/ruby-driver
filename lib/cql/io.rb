# encoding: utf-8

module Cql
  module Io
    IoError = Class.new(CqlError)
    ConnectionError = Class.new(IoError)
    NotRunningError = Class.new(CqlError)
    ConnectionNotFoundError = Class.new(CqlError)
    ConnectionBusyError = Class.new(CqlError)
  end
end

require 'cql/io/io_reactor'
