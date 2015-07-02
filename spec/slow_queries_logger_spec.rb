
require 'spec_helper'

module Cassandra
  describe SlowQueriesLogger do
    subject { described_class.new logger }

    let(:req_id) { 1 }
    let(:cql) { "SELECT * FROM here" }
    let(:request) { double('request', cql: cql) }

    let(:logger) { instance_double('Logger', warn: true, debug: true) }

    describe '#start' do
      it 'saves the start time' do
        expect do
          subject.start req_id, request
        end.to change { subject.instance_variable_get(:@times).keys.count }.from(0).to 1
      end

      it 'saves the request' do
        expect do
          subject.start req_id, request
        end.to change { subject.instance_variable_get(:@requests)[req_id] }.from(nil).to cql
      end
    end

    describe '#finish' do
      it 'logs if neccessary' do
        subject.start req_id, request
        sleep 0.3
        expect(logger).to receive(:warn).twice
        subject.finish req_id
      end

      it "doesn't log if not neccessary" do
        subject.start req_id, request
        expect(logger).to_not receive(:warn)
        subject.finish req_id
      end
    end

    describe '#delete' do
      before { subject.start req_id, request }
      it 'deletes the time record' do
        expect do
          subject.delete req_id
        end.to change { subject.instance_variable_get(:@times).keys.count }.from(1).to 0
      end

      it 'deletes the request' do
        expect do
          subject.delete req_id
        end.to change { subject.instance_variable_get(:@requests)[req_id] }.from(cql).to nil
      end
    end

    describe 'queries for logger method convenience' do
      it 'returns a null queries logger if null logger is given' do
        expect(Cassandra.queries_logger_for NullLogger.new).to be_a NullQueriesLogger
      end

      it 'returns a slow queries logger otherwise' do
        expect(Cassandra.queries_logger_for Logger.new(STDOUT)).to be_a SlowQueriesLogger
      end
    end
  end
end
