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

module Cql
  module Client
    # @private
    class NullLogger
      def close(*); end
      def debug(*); end
      def debug?; false end
      def error(*); end
      def error?; false end
      def fatal(*); end
      def fatal?; false end
      def info(*); end
      def info?; false end
      def unknown(*); end
      def warn(*); end
      def warn?; false end
    end
  end
end
