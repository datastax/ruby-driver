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

require 'spec_helper'

module Cql
  module Execution
    describe(Trace) do
      let(:id)     { TimeUuid::Generator.new.next }
      let(:client) { double('client') }
      let(:trace)  { Trace.new(id, client) }

      [:coordinator, :duration, :parameters, :request, :started_at].each do |method|
        describe("##{method}") do
          let(:statement) { Statements::Simple.new("SELECT * FROM system_traces.sessions WHERE session_id = ?", id) }
          let(:data) do
            {
              'coordinator' => IPAddr.new('127.0.0.1'),
              'duration'    => nil,
              'parameters'  => {'page_size' => '50000', 'query' => 'SELECT * FROM songs'},
              'request'     => 'Execute CQL3 query',
              'started_at'  => Time.now
            }
          end
          let(:rows) { [data] }
          let(:future_rows) { Futures::Fulfilled.new(rows) }

          it "loads #{method} from system_traces.sessions" do
            expect(client).to receive(:query).once.with(statement, VOID_OPTIONS).and_return(future_rows)
            expect(trace.__send__(method)).to eq(data[method.to_s])
          end

          it "loads #{method} only once" do
            expect(client).to receive(:query).once.with(statement, VOID_OPTIONS).and_return(future_rows)
            10.times { expect(trace.__send__(method)).to eq(data[method.to_s]) }
          end
        end
      end

      describe('#events') do
        let(:statement) { Statements::Simple.new("SELECT * FROM system_traces.events WHERE session_id = ?", id) }
        let(:rows) do
          generator = TimeUuid::Generator.new

          5.times.map do |i|
            {
              'event_id'       => double("event_id ##{i + 1}"),
              'activity'       => double("activity ##{i + 1}"),
              'source'         => double("source ##{i + 1}"),
              'source_elapsed' => double("source_elapsed ##{i + 1}"),
              'thread'         => double("thread ##{i + 1}")
            }
          end
        end
        let(:future_rows) { Futures::Fulfilled.new(rows) }

        it "loads events from system_traces.events" do
          expect(client).to receive(:query).once.with(statement, VOID_OPTIONS).and_return(future_rows)
          expect(trace.events).to have(5).events
        end

        it "loads events only once" do
          expect(client).to receive(:query).once.with(statement, VOID_OPTIONS).and_return(future_rows)
          10.times { expect(trace.events).to have(5).events }
        end
      end
    end
  end
end
