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

class TryNextHostRetryPolicy
  include Cassandra::Retry::Policy

  def read_timeout(statement, consistency_level, required_responses, received_responses, data_retrieved, retries)
    try_next_host
  end

  def write_timeout(statement, consistency_level, write_type, acks_required, acks_received, retries)
    try_next_host
  end

  def unavailable(statement, consistency_level, replicas_required, replicas_alive, retries)
    try_next_host
  end
end