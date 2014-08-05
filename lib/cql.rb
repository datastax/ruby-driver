# encoding: utf-8

require 'ione'

require 'monitor'
require 'ipaddr'
require 'set'
require 'bigdecimal'
require 'forwardable'

module Cql
  CqlError = Class.new(StandardError)
  IoError = Ione::IoError

  # @private
  Io = Ione::Io

  CONSISTENCIES  = [:any, :one, :two, :three, :quorum, :all, :local_quorum, :each_quorum, :serial, :local_serial, :local_one].freeze
  # @private
  SERIAL_CONSISTENCIES = [:serial, :local_serial].freeze

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

require 'cql/promise'
require 'cql/builder'
require 'cql/cluster'
require 'cql/driver'
require 'cql/host'
require 'cql/reactor'
require 'cql/session'
require 'cql/results'
require 'cql/statement'
require 'cql/statements'

require 'cql/execution/info'
require 'cql/execution/options'
require 'cql/execution/trace'

require 'cql/load_balancing'
require 'cql/reconnection'
require 'cql/retry'

module Cql
  # @private
  VOID_STATEMENT = Statements::Void.new
  # @private
  VOID_OPTIONS   = Execution::Options.new({:consistency => :one})
  # @private
  NO_HOSTS       = NoHostsAvailable.new
end
