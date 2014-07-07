# encoding: utf-8

require 'ione'
require 'monitor'

module Cql
  CqlError = Class.new(StandardError)
  IoError = Ione::IoError

  # @private
  Promise = Ione::Promise

  # @private
  Future = Ione::Future

  Future.__send__(:alias_method, :get, :value)

  # @private
  Io = Ione::Io

  def self.cluster
    Builder.new
  end
end

require 'cql/uuid'
require 'cql/time_uuid'
require 'cql/compression'
require 'cql/protocol'
require 'cql/auth'
require 'cql/client'

require 'cql/builder'
require 'cql/cluster'
require 'cql/session'
require 'cql/thread_safe'
