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

# Use this module to mark domain object classes as custom type implementations for custom-type
# columns in C*. This module has no logic of its own, but indicates that the marked class has
# certain methods.
# @private
module Cassandra
  module CustomData
    def self.included(base)
      base.send :include, InstanceMethods
      base.extend ClassMethods
    end

    module ClassMethods
      # @return [Cassandra::Types::Custom] the custom type that this class represents.
      def type
        raise NotImplementedError, "#{self.class} must implement the :type class method"
      end

      # Deserialize the given data into an instance of this domain object class.
      # @param data [String] byte-array representation of a column value of this custom type.
      # @return An instance of the domain object class.
      # @raise [Cassandra::Errors::DecodingError] upon failure.
      def deserialize(data)
        raise NotImplementedError, "#{self.class} must implement the :deserialize class method"
      end
    end

    module InstanceMethods
      # Serialize this domain object into a byte array to send to C*.
      # @return [String] byte-array representation of this domain object.
      def serialize
        raise NotImplementedError, "#{self.class} must implement the :serialize instance method"
      end
    end
  end
end
