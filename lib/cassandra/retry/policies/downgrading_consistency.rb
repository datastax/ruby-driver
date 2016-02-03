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
  module Retry
    module Policies
      class DowngradingConsistency
        include Policy

        def read_timeout(statement, consistency, required, received, retrieved, retries)
          return reraise if retries > 0 || SERIAL_CONSISTENCIES.include?(consistency)
          if received < required
            return max_likely_to_work(consistency, required, received)
          end

          retrieved ? reraise : try_again(consistency)
        end

        def write_timeout(statement, consistency, type, required, received, retries)
          return reraise if retries > 0

          case type
          when :simple, :batch
            ignore
          when :unlogged_batch
            max_likely_to_work(consistency, required, received)
          when :batch_log
            try_again(consistency)
          else
            reraise
          end
        end

        def unavailable(statement, consistency, required, alive, retries)
          return reraise if retries > 0

          max_likely_to_work(consistency, required, alive)
        end

        private

        def max_likely_to_work(consistency, required, received)
          if consistency == :all &&
             required > 1 &&
             received >= (required.to_f / 2).floor + 1
            try_again(:quorum)
          elsif received >= 3
            try_again(:three)
          elsif received >= 2
            try_again(:two)
          elsif received >= 1
            try_again(:one)
          else
            reraise
          end
        end
      end
    end
  end
end
