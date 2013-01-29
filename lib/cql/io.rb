# encoding: utf-8

module Cql
  module Io
    IoError = Class.new(CqlError)
    ConnectionError = Class.new(IoError)
    IllegalStateError = Class.new(IoError)
  end
end

require 'cql/io/connection'
