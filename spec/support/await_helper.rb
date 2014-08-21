# encoding: utf-8

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

module AwaitHelper
  def await(timeout=5, &test)
    started_at = Time.now
    until test.call
      yield
      time_taken = Time.now - started_at
      if time_taken > timeout
        fail('Test took more than %.1fs' % [time_taken.to_f])
      else
        sleep(0.01)
      end
    end
  end
end

RSpec.configure do |c|
  c.include(AwaitHelper)
end