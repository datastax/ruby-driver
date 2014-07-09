# encoding: utf-8

module Cql
  class Host
    attr_reader :ip, :id, :rack, :datacenter, :release_version, :status

    def initialize(ip, id, rack, datacenter, release_version, status)
      @ip              = ip
      @id              = id
      @rack            = rack
      @datacenter      = datacenter
      @release_version = release_version
      @status          = status
    end
  end
end
