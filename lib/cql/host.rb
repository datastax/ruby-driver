# encoding: utf-8

module Cql
  class Host
    attr_reader :ip, :rack, :datacenter, :release_version

    def initialize(ip, rack, datacenter, release_version)
      @ip              = ip
      @rack            = rack
      @datacenter      = datacenter
      @release_version = release_version
    end
  end
end
