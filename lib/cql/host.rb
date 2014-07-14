# encoding: utf-8

module Cql
  class Host
    attr_reader :ip, :id, :rack, :datacenter, :release_version, :status

    def initialize(ip, id = nil, rack = nil, datacenter = nil, release_version = nil, status = :up)
      @ip              = ip
      @id              = id
      @rack            = rack
      @datacenter      = datacenter
      @release_version = release_version
      @status          = status
    end

    def up?
      @status == :up
    end

    def down?
      @status == :down
    end
  end
end
