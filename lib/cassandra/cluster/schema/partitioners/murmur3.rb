# encoding: utf-8

#--
# Copyright 2013-2015 DataStax, Inc.
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
  class Cluster
    class Schema
      # @private
      module Partitioners
        # @private
        class Murmur3
          def create_token(partition_key)
            token = Cassandra::Murmur3.hash(partition_key)
            token = LONG_MAX if token == LONG_MIN

            token
          end

          def parse_token(token_string)
            token_string.to_i
          end

          private

          # @private
          LONG_MIN = -2 ** 63
          # @private
          LONG_MAX = 2 ** 63 - 1
        end
      end
    end
  end
end
