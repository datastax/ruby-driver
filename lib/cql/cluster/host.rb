# encoding: utf-8

module Cql
  class Cluster
    class Host
      attr_reader   :ip, :status
      attr_accessor :id, :rack, :datacenter, :release_version

      def initialize(ip, data = {})
        @ip              = ip
        @id              = data['host_id']
        @release_version = data['release_version']
        @rack            = data['rack']
        @datacenter      = data['data_center']
        @status          = :up
      end

      def up?
        @status == :up
      end

      def up!
        @status = :up
        self
      end

      def down?
        @status == :down
      end

      def down!
        @status = :down
        self
      end
    end
  end
end
