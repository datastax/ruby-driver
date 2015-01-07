# encoding: utf-8

#--
# Copyright 2013-2014 DataStax, Inc.
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
    class StatusChangeEventResponse < EventResponse
      TYPE = 'STATUS_CHANGE'.freeze

      attr_reader :type, :change, :address, :port

      def initialize(*args)
        @change, @address, @port = args
        @type = TYPE
      end

      def to_s
        %(EVENT #@type #@change #@address:#@port)
      end

      private

      EVENT_TYPES[TYPE] = self
    end
  end
end
