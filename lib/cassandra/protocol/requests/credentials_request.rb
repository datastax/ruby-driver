# encoding: utf-8

#--
# Copyright 2013-2016 DataStax, Inc.
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
    class CredentialsRequest < Request
      attr_reader :credentials

      def initialize(credentials)
        super(4)
        @credentials = credentials.dup.freeze
      end

      def write(buffer, protocol_version, encoder)
        buffer.append_string_map(@credentials)
      end

      def to_s
        %(CREDENTIALS #{@credentials})
      end

      def eql?(rq)
        rq.is_a?(self.class) && rq.credentials.eql?(@credentials)
      end
      alias == eql?

      def hash
        @h ||= begin
          h = 17
          h = 31 * h + @credentials.hash
          h
        end
      end
    end
  end
end
