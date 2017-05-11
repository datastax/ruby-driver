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

require 'spec_helper'

def make_options(logger,
                 protocol_version,
                 connections_per_local_node,
                 connections_per_remote_node,
                 requests_per_connection,
                 allow_boolean_protocol)
  Cassandra::Cluster::Options.new(
      logger, protocol_version, nil, nil, nil, nil, nil, false,
      connections_per_local_node, connections_per_remote_node, 60, 30, true, 1, 10,
      true, requests_per_connection, [], allow_boolean_protocol)
end

module Cassandra
  class Cluster
    describe(Options) do
      let(:logger) { Cassandra::NullLogger.new }
      it 'should set the protocol-version to max-supported if not in beta' do
        expect(make_options(logger, nil, nil, nil, nil, false).
            protocol_version).to eq(Cassandra::Protocol::Versions::MAX_SUPPORTED_VERSION)
      end

      it 'should set the protocol-version to beta version if allow-beta is true' do
        expect(make_options(logger, nil, nil, nil, nil, true).
            protocol_version).to eq(Cassandra::Protocol::Versions::BETA_VERSION)
      end

      context :connections_per_local_node do
        it 'should return the default value for v2' do
          expect(make_options(logger, 2, nil, nil, nil, false).
              connections_per_local_node).to eq(2)
        end

        it 'should return the default value for v3' do
          expect(make_options(logger, 3, nil, nil, nil, false).
              connections_per_local_node).to eq(1)
        end

        it 'should return the user-specified value' do
          expect(make_options(logger, 3, 12, nil, nil, false).
              connections_per_local_node).to eq(12)
          expect(make_options(logger, 2, 13, nil, nil, false).
              connections_per_local_node).to eq(13)
        end
      end

      context :connections_per_remote_node do
        it 'should return the default value' do
          expect(make_options(logger, 2, nil, nil, nil, false).
              connections_per_remote_node).to eq(1)
          expect(make_options(logger, 3, nil, nil, nil, false).
              connections_per_remote_node).to eq(1)
        end

        it 'should return the user-specified value' do
          expect(make_options(logger, 3, nil, 14, nil, false).
              connections_per_remote_node).to eq(14)
        end
      end

      context :requests_per_connection do
        let(:logger) { double(Cassandra::NullLogger) }

        it 'should return the default value for v2' do
          expect(logger).to_not receive(:warn)
          expect(make_options(logger, 2, nil, nil, nil, false).
              requests_per_connection).to eq(128)
        end
        it 'should return the default value for v3' do
          expect(logger).to_not receive(:warn)
          expect(make_options(logger, 3, nil, nil, nil, false).
              requests_per_connection).to eq(1024)
        end
        it 'should return the user-specified value' do
          expect(logger).to_not receive(:warn)
          expect(make_options(logger, 2, nil, nil, 13, false).
              requests_per_connection).to eq(13)
          expect(make_options(logger, 3, nil, nil, 14, false).
              requests_per_connection).to eq(14)
        end

        it 'should pull down the value to 128 if requested value is too high for v2' do
          expect(logger).to receive(:warn)
          expect(make_options(logger, 2, nil, nil, 150, false).
              requests_per_connection).to eq(128)
        end

        it 'should not adjust high value for v3' do
          expect(logger).to_not receive(:warn)
          expect(make_options(logger, 3, nil, nil, 150, false).
              requests_per_connection).to eq(150)
        end
      end
    end
  end
end
