# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe QueryTrace do
      let :trace do
        described_class.new(session_row, event_rows)
      end

      let :started_at do
        Time.now
      end

      let :session_row do
        {
          'session_id' => Uuid.new('a1028490-3f05-11e3-9531-fb72eff05fbb'),
          'coordinator' => IPAddr.new('127.0.0.1'),
          'duration' => 1263,
          'parameters' => {
            'query' => 'SELECT * FROM something'
          },
          'request' => 'Execute CQL3 query',
          'started_at' => started_at
        }
      end

      let :event_rows do
        [
          {
            'session_id' => Uuid.new('a1028490-3f05-11e3-9531-fb72eff05fbb'),
            'event_id' => TimeUuid.new('a1028491-3f05-11e3-9531-fb72eff05fbb'),
            'activity' => 'Parsing statement',
            'source' => IPAddr.new('127.0.0.1'),
            'source_elapsed' => 52,
            'thread' => 'Native-Transport-Requests:126'
          },
          {
            'session_id' => Uuid.new('a1028490-3f05-11e3-9531-fb72eff05fbb'),
            'event_id' => TimeUuid.new('a1028492-3f05-11e3-9531-fb72eff05fbb'),
            'activity' => 'Peparing statement',
            'source' => IPAddr.new('127.0.0.1'),
            'source_elapsed' => 54,
            'thread' => 'Native-Transport-Requests:126'
          },
        ]
      end

      context 'when the session is nil' do
        it 'returns nil from all methods' do
          trace = described_class.new(nil, nil)
          trace.coordinator.should be_nil
          trace.cql.should be_nil
          trace.started_at.should be_nil
          trace.events.should be_empty
        end
      end

      context 'when the duration field of the session is nil' do
        it 'raises an IncompleteTraceError' do
          session_row['duration'] = nil
          expect { described_class.new(session_row, event_rows) }.to raise_error(IncompleteTraceError)
        end
      end

      describe '#coordinator' do
        it 'returns the IP address of the coordinator node' do
          trace.coordinator.should == IPAddr.new('127.0.0.1')
        end
      end

      describe '#cql' do
        it 'returns the query' do
          trace.cql.should == 'SELECT * FROM something'
        end
      end

      describe '#started_at' do
        it 'returns the time the request started' do
          trace.started_at.should eql(started_at)
        end
      end

      describe '#duration' do
        it 'returns the duration in seconds' do
          trace.duration.should == 0.001263
        end
      end

      describe '#events' do
        it 'returns a list of TraceEvents' do
          trace.events.should have(2).items
          trace.events.first.should be_a(TraceEvent)
        end

        it 'returns an unmodifiable list' do
          expect { trace.events << :foo }.to raise_error
        end

        context 'returns a list of trace events whose' do
          let :events do
            trace.events
          end

          let :event do
            events.first
          end

          describe '#activity' do
            it 'returns the event activity' do
              event.activity.should == 'Parsing statement'
            end
          end

          describe '#source' do
            it 'returns the event source' do
              event.source.should == IPAddr.new('127.0.0.1')
            end
          end

          describe '#source_elapsed' do
            it 'returns the elapsed time at the source' do
              event.source_elapsed.should == 0.000052
            end
          end

          describe '#time' do
            it 'returns the time component from the event ID' do
              event.time.to_i.should == TimeUuid.new('a1028492-3f05-11e3-9531-fb72eff05fbb').to_time.to_i
            end
          end
        end
      end
    end
  end
end