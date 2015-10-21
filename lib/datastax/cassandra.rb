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

module DataStax
  @base = __FILE__ + '/../..'

  def self.require(path)
    if path.start_with?('cassandra/')
      include(path)
    else
      ::Kernel.require(path)
    end
  end

  def self.include(path)
    path = File.expand_path(path + '.rb', @base)
    class_eval(File.read(path), path, 1)
  end

  previous = nil
  murmur3  = nil
  if defined?(::Cassandra)
    previous = ::Cassandra
    murmur3  = ::Cassandra::Murmur3
    Object.send(:remove_const, :Cassandra)
  end
  include 'cassandra'
  murmur3 ||= ::Cassandra::Murmur3
  DataStax::Cassandra::Murmur3 = murmur3
  Object.send(:remove_const, :Cassandra) if defined?(::Cassandra)
  ::Cassandra = previous if previous
end
