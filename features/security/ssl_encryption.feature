@ssl
Feature: SSL encryption

  There are a few levels of security that SSL encryption can provide when
  communicating with Apache Cassandra. You can find them below in the
  increasing security order: the first being the least secure and the last -
  the most.

  1. Using default SSL context. This method is actually not secure as there is
     no peer validation happening. `Cassandra.connect(ssl: true)`
  2. Using shared certificate authority. This method is more secure than the
     previous one, because the client is able to validate the identity of the
     server. Using this method the server can't trust the client unless
     additional authentication has been provided.
     `Cassandra.connect(server_cert: '/path/to/ca.pem')`
  3. Using trusted client certificates. This method, in addition to peer
     verification, allows server to validate identities of the clients. Make
     sure to enable `client_encryption_options.require_client_auth` setting in
     `cassandra.yaml`, as well as add all trusted client keys to Apache
     Cassandra's truststore. You can use server certificate as well as client
     certificate and private key with an optional passphrase to connect to
     Apache Cassandra. `Cassandra.connect(server_cert: '/path/to/ca.pem', client_cert: '/path/to/client.pem', private_key: '/path/to/client.key', passphrase:  'secret passphrase for private key')`

  Scenario: Using default SSL encryption
    Given a running cassandra cluster with SSL encryption enabled
    And the following example:
      """ruby
      require 'cassandra'
      
      begin
        cluster = Cassandra.connect(ssl: true)
        puts "connection successful"
      rescue => e
        puts "#{e.class.name}: #{e.message}"
        puts "connection failed"
      else
        cluster.close
      end
      """
    When it is executed
    Then its output should contain:
      """
      connection successful
      """

  Scenario: Connection fails when not using SSL encryption
    Given a running cassandra cluster with SSL encryption enabled
    And the following example:
      """ruby
      require 'cassandra'
      
      begin
        cluster = Cassandra.connect(ssl: false, connect_timeout: 2)
        puts "connection successful"
      rescue => e
        puts "#{e.class.name}: #{e.message}"
        puts "connection failed"
      else
        cluster.close
      end
      """
    When it is executed
    Then its output should contain:
      """
      connection failed
      """

  Scenario: Using an SSL certificate authority
    Given a running cassandra cluster with SSL encryption enabled
    And the following example:
      """ruby
      require 'cassandra'

      begin
        cluster = Cassandra.connect(server_cert: ENV['SERVER_CERT'])
        puts "connection successful"
      rescue => e
        puts "#{e.class.name}: #{e.message}"
        puts "connection failed"
      else
        cluster.close
      end
      """
    When it is executed with a valid ca path in the environment
    Then its output should contain:
      """
      connection successful
      """

  Scenario: Using SSL authentication
    Given a running cassandra cluster with SSL client authentication enabled
    And the following example:
      """ruby
      require 'cassandra'

      begin
        cluster = Cassandra.connect(
          server_cert:  ENV['SERVER_CERT'],
          client_cert:  ENV['CLIENT_CERT'],
          private_key:  ENV['PRIVATE_KEY'],
          passphrase:   ENV['PASSPHRASE']
        )
        puts "connection successful"
      rescue => e
        puts "#{e.class.name}: #{e.message}"
        puts "connection failed"
      else
        cluster.close
      end
      """
    When it is executed with ca and cert path and key in the environment
    Then its output should contain:
      """
      connection successful
      """

  Scenario: Using a custom SSL context
    Given a running cassandra cluster with SSL encryption enabled
    And the following example:
      """ruby
      require 'cassandra'
      require 'openssl'

      begin
        cluster = Cassandra.connect(ssl: OpenSSL::SSL::SSLContext.new)
        puts "connection successful"
      rescue => e
        puts "#{e.class.name}: #{e.message}"
        puts "connection failed"
      else
        cluster.close
      end
      """
    When it is executed
    Then its output should contain:
      """
      connection successful
      """
