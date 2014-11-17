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

module Cassandra
  module Execution
    class Trace
      class Event
        # @return [Cassandra::Uuid] event uuid
        attr_reader :id

        # @return [String] description of activity
        attr_reader :activity

        attr_reader :source
        attr_reader :source_elapsed
        attr_reader :thread

        # @private
        def initialize(id, activity, source, source_elapsed, thread)
          @id             = id
          @activity       = activity
          @source         = source
          @source_elapsed = source_elapsed
          @thread         = thread
        end

        def ==(other)
          other == @id
        end

        alias :eql? :==
      end

      include MonitorMixin

      # @return [Cassandra::Uuid] trace id
      attr_reader :id

      # @private
      def initialize(id, client)
        @id     = id
        @client = client

        mon_initialize
      end

      # Returns the ip of coordinator node. Typically the same as {Cassandra::Execution::Info#hosts}`.last`
      #
      # @return [IPAddr] ip of the coordinator node
      def coordinator
        load unless @coordinator

        @coordinator
      end

      def duration
        load unless @duration

        @duration
      end

      def parameters
        load unless @parameters

        @parameters
      end

      def request
        load unless @request

        @request
      end

      def started_at
        load unless @started_at

        @started_at
      end

      # Returns all trace events
      #
      # @return [Array<Cassandra::Execution::Trace::Event>] events
      def events
        load_events unless @events

        @events
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)} @id=#{@id.inspect}>"
      end

      private

      # @private
      SELECT_SESSION = "SELECT * FROM system_traces.sessions WHERE session_id = %s"
      # @private
      SELECT_EVENTS  = "SELECT * FROM system_traces.events WHERE session_id = %s"

      # @private
      def load
        synchronize do
          return if @loaded

          data = @client.query(Statements::Simple.new(SELECT_SESSION % @id), VOID_OPTIONS).get.first
          raise ::RuntimeError, "unable to load trace #{@id}" if data.nil?

          @coordinator = data['coordinator']
          @duration    = data['duration']
          @parameters  = data['parameters']
          @request     = data['request']
          @started_at  = data['started_at']
          @loaded      = true
        end

        nil
      end

      # @private
      def load_events
        synchronize do
          return if @loaded_events

          @events = []

          @client.query(Statements::Simple.new(SELECT_EVENTS % @id), VOID_OPTIONS).get.each do |row|
            @events << Event.new(row['event_id'], row['activity'], row['source'], row['source_elapsed'], row['thread'])
          end

          @events.freeze

          @loaded_events = true
        end
      end
    end
  end
end
