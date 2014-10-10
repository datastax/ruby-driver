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

require File.dirname(__FILE__) + '/../integration_test_case.rb'

class SSLEncryptionTest < IntegrationTestCase
  def self.before_suite
    super
    @@server_cert = @@ccm_cluster.enable_ssl
  end

  def self.after_suite
    @@ccm_cluster && @@ccm_cluster.disable_ssl
    super
  end

  def test_can_connect_with_default_ssl
    cluster = Cassandra.cluster(ssl: true)
    refute_nil cluster
  ensure
    cluster.close
  end

  def test_raise_error_when_not_using_ssl
    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      cluster = Cassandra.cluster
      cluster.close
    end
  end

  def test_can_connect_with_ssl_ca
    cluster = Cassandra.cluster(server_cert: @@server_cert)
    refute_nil cluster
  ensure
    cluster.close
  end

  def test_raise_error_on_invalid_ca_provided
    assert_raises(Cassandra::Errors::NoHostsAvailable) do
      cluster = Cassandra.cluster(server_cert: '')
      cluster.close
    end
  end
end