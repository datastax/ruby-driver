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

require 'stringio'

module Cassandra
  # @private
  module Util extend self
    def encode_hash(hash, io = StringIO.new)
      first = true

      io.putc('{')
      hash.each do |k, v|
        if first
          first = false
        else
          io.print(', ')
        end

        encode_string(k, io)
        io.print(': ')
        encode_object(v, io)
      end
      io.putc('}')

      io.string
    end

    def encode_array(array, io = StringIO.new)
      first = true

      io.putc('[')
      array.each_with_index do |object, i|
        if first
          first = false
        else
          io.print(', ')
        end

        encode_object(object, io)
      end
      io.putc(']')

      io.string
    end

    def encode_string(string, io = StringIO.new)
      io.putc(?')
      string.chars do |c|
        case c
        when ?'
          io.print("\\'")
        else
          io.putc(c)
        end
      end
      io.putc(?')

      io.string
    end

    def encode_object(object, io = StringIO.new)
      case object
      when Hash    then encode_hash(object, io)
      when Array   then encode_array(object, io)
      when String  then encode_string(object, io)
      when Numeric then io.print(object)
      when true    then io.print('true')
      when false   then io.print('false')
      when nil     then io.print('null')
      end

      io.string
    end

    def escape_name(name)
      return name if name[LOWERCASE_REGEXP] == name
      '"' + name + '"'
    end

    # @private
    LOWERCASE_REGEXP = /[[:lower:]\_]*/
  end
end
