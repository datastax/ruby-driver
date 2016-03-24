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

# This file monkey-patches Module to have an attr_boolean method to make it easy
# for classes to define boolean instance variables with "foo?" reader methods.
# Inspired by http://stackoverflow.com/questions/4013591/attr-reader-with-question-mark-in-a-name
module Cassandra
  module AttrBoolean
    def attr_boolean(*names)
      names.each do |name|
        define_method(:"#{name}?") do
          res = instance_variable_get(:"@#{name}")
          !res.nil? && res
        end
      end
    end
  end
end
