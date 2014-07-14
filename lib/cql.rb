# encoding: utf-8

require 'ione'

require 'monitor'
require 'ipaddr'
require 'set'
require 'bigdecimal'

module Cql
  CqlError = Class.new(StandardError)
  IoError = Ione::IoError

  # @private
  Promise = Ione::Promise

  # @private
  Future = Ione::Future

  class Future
    def get
      value
    end
  end

  # @private
  Io = Ione::Io

  VOID_STATEMENT = nil

  def self.cluster(defaults = {})
    Builder.new(defaults)
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
require 'cql/driver'
require 'cql/host'
require 'cql/session'
require 'cql/thread_safe'

require 'cql/load_balancing'