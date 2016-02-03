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

require 'logger'
module Cassandra
  # This class is a logger that may be used by the client to log the driver's actions.
  # It is a subclass of the standard Ruby Logger class, so it is instantiated the
  # same way.
  #
  # The format of log output is set to include the timestamp, thread-id, log severity,
  # and message. <b>This format may change in newer versions of the driver to account
  # for new/deprecated metadata.</b>
  #
  # @example Configuring {Cassandra::Cluster} to use a logger.
  #   cluster = Cassandra.cluster(logger: Cassandra::Logger.new($stderr))
  #
  # @example The log format may be changed the same way as in the standard Ruby Logger class
  #   logger = Cassandra::Logger.new($stderr)
  #   logger.formatter = proc { |severity, time, program_name, message|
  #     "[%s]: %s\n" % [severity, message]
  #   }
  #
  # @example Create a logger and use it in your own business logic
  #   logger = Cassandra::Logger.new($stderr)
  #   cluster = Cassandra.cluster(logger: logger)
  #   <various logic>
  #   logger.debug("something interesting happened.")

  class Logger < ::Logger
    # @private
    # This class is mostly copied from the Ruby Logger::Format class.
    class Formatter
      Format = "[%s#%d] %5s: %s\n".freeze

      def call(severity, time, _, msg)
        Format % [format_datetime(time), Thread.current.object_id, severity,
                  msg2str(msg)]
      end

      def format_datetime(time)
        time.strftime('%H:%M:%S.') << '%06d ' % time.usec
      end

      def msg2str(msg)
        case msg
        when ::String
          msg
        when ::Exception
          "#{msg.message} (#{msg.class})\n" <<
            (msg.backtrace || []).join("\n")
        else
          msg.inspect
        end
      end
    end

    def initialize(*args)
      super(*args)
      self.formatter = Formatter.new
    end
  end
end
