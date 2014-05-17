# encoding: utf-8

require 'ione'


module Cql
  CqlError = Class.new(StandardError)

  Promise = Ione::Promise
  Future = Ione::Future
  Io = Ione::Io
  IoError = Ione::IoError
end

require 'cql/uuid'
require 'cql/time_uuid'
require 'cql/compression'
require 'cql/protocol'
require 'cql/auth'
require 'cql/client'
