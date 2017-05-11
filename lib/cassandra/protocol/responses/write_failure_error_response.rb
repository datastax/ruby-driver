# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

module Cassandra
  module Protocol
    class WriteFailureErrorResponse < ErrorResponse
      attr_reader :consistency, :received, :blockfor, :numfailures, :write_type, :failures_by_node

      def initialize(custom_payload,
                     warnings,
                     code,
                     message,
                     consistency,
                     received,
                     blockfor,
                     numfailures,
                     write_type,
                     failures_by_node)
        super(custom_payload, warnings, code, message)

        write_type.downcase!

        @consistency      = consistency
        @received         = received
        @blockfor         = blockfor
        @write_type       = write_type.to_sym
        @failures_by_node = failures_by_node

        # If failures_by_node is set, numfailures isn't, and v.v. Set @numfailures to the size of the failure-map
        # if numfailures is nil.
        @numfailures = numfailures || @failures_by_node.size
      end

      def to_error(keyspace, statement, options, hosts, consistency, retries)
        Errors::WriteError.new(@message,
                               @custom_payload,
                               @warnings,
                               keyspace,
                               statement,
                               options,
                               hosts,
                               consistency,
                               retries,
                               @write_type,
                               @consistency,
                               @blockfor,
                               @numfailures,
                               @received,
                               @failures_by_node)
      end

      def to_s
        "#{super} #{@write_type} #{@consistency} #{@blockfor} " \
            "#{@numfailures} #{@received}"
      end
    end
  end
end
