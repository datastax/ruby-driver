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

class SSLAuthenticatedEncryptionTest < IntegrationTestCase
  def setup
    @server_cert, @client_cert, @private_key, @passphrase = ccm_cluster.enable_ssl_client_auth
  end

  def teardown
    ccm_cluster.disable_ssl
  end

  def test_can_connect_with_ssl_authentication
    cluster = Cassandra.connect(
                server_cert:  @server_cert,
                client_cert:  @client_cert,
                private_key:  @private_key,
                passphrase:   @passphrase
              )
    refute_nil cluster
    cluster.close
  end

  def test_raise_error_on_invalid_ssl_auth
    assert_raises(OpenSSL::PKey::RSAError) do
      cluster = Cassandra.connect(
                  server_cert:  @server_cert,
                  client_cert:  @client_cert,
                  private_key:  @private_key,
                  passphrase:   'badpassword'
                )
      cluster.close
    end
  end
end