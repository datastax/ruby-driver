# Security

## Authentication

Out of the box, Ruby driver supports [Cassandra's internal authentication mechanism](http://www.datastax.com/documentation/cassandra/2.0/cassandra/security/security_config_native_authenticate_t.html). It is also possible to provide a custom authenticator implementation, refer to [`Cassandra::Auth::Provider`](http://datastax.github.io/ruby-driver/api/auth/provider/) for more information.

## SSL encryption

[Apache Cassandra supports client to node encryption](http://www.datastax.com/documentation/cassandra/1.2/cassandra/security/secureSSLClientToNode_t.html) and even [trusted clients](http://www.datastax.com/documentation/cassandra/1.2/cassandra/configuration/configCassandra_yaml_r.html?scroll=reference_ds_qfg_n1r_1k__client_encryption_options_unique_1) (starting with 1.2.3)

### Setting up SSL encryption for Apache Cassandra

Setting up SSL encryption for Apache Cassandra may seem borderline impossible for someone unfamiliar with the process. I know, I've been there. Let me guide you through it.

#### Installing Java Cryptography Extension

Before we begin, we have to install Java Cryptography Extension (JCE). Feel free to skip this step if you already have it. Without JCE, cassandra processes will fail to start after enabling encryption.

1. [Download JCE from Oracle](http://www.oracle.com/technetwork/java/javase/downloads/jce-7-download-432124.html)
2. Extract files from the downloaded archive.
3. Copy `local_policy.jar` and `US_export_policy.jar` to `$JAVA_HOME/jre/lib/security`

Below are some tips for finding `$JAVA_HOME` if you don't know how to.

##### Mac OS X

```bash
JAVA_HOME=$(/usr/libexec/java_home)
```

##### Ubuntu

```bash
JAVA_HOME=$(readlink -f /usr/bin/javac | sed "s:/bin/javac::")
```

#### Setting up cassandra keystore

First, we're going to have to create a server certificate. It is recommended to use a different certificate for every node that we want to secure communication with using SSL encryption:

```bash
node_alias=node1
store_pass="some very long and secure password"

keytool -genkeypair -noprompt \
  -keyalg RSA \
  -validity 36500 \
  -alias "$node_alias" \
  -keystore "$node_alias/conf/.keystore" \
  -storepass "$store_pass" \
  -dname "CN=Cassandra Server, OU=Ruby Driver Tests, O=DataStax Inc., L=Santa Clara, ST=California, C=US"
```

Once we've run the script above for all nodes in our cluster, we can configure our Apache Cassandra servers to use their respective `.keystore`s. For this, we'll have to modify `cassandra.yaml` to include the following:

```yaml
client_encryption_options:
  enabled: true
  keystore: conf/.keystore
  keystore_password: "some very long and secure password"
```

__Note__: The values of `keystore` and `keystore_password` above must be the same as the values of `-keystore` and `storepass` options from the previous shell script.

Now you can restart your cassandra processes.

At this point you already have SSL enabled and can even connect to the servers using DataStax Ruby Driver:

```ruby
require 'cassandra'

cluster = Cassandra.cluster(ssl: true)
```

This however is like having no security at all since the driver won't be able to verify the identity of the server.

#### Extracting server certificate for peer verification

There are several ways to have client verify server's identity. I'm going to extract a PEM certificate of the server, which is suitable for use with the OpenSSL library that the Ruby Driver uses, and give it to the client for verification.

Let's extract a PEM certificate out of our server's keystore:

```bash
# values same as above
node_alias=node1
store_pass="some very long and secure password"

keytool -exportcert -noprompt \
  -rfc \
  -alias "$node_alias" \
  -keystore "$node_alias/conf/.keystore" \
  -storepass "$store_pass" \
  -file "$node_alias".pem

chmod 400 "$node_alias".pem
```

__Note__: The values of `-alias`, `-keystore` and `-storepass` options must be the same as in the script used to generate the keystore file.

This process has to be repeated for each unique keystore that we created in the very first section of this guide.

Once we have all PEM certificates exported we must bundle them for the client to use:

```
cat node1.pem node2.pem node3.pem > server.pem
```

__Note__: You can skip this step if you aleady have only one pem certificate.

Finally, having our combined PEM certificate, we can give it to the client to verify our server's identity:

```ruby
require 'cassandra'

cluster = Cassandra.cluster(server_cert: '/path/to/server.pem')
```

This is better, as our client can now verify the identity of the server. And, combined with Standard Authentication, this provides enough security to be useful. However, we can do better still.

#### Enabling SSL Authentication and trusted clients

If you've gotten this far, you are really serious about security, well done. The great thing is so is Apache Cassandra. Here we'll learn how to enable SSL authentication for your Apache Cassandra setup.

Enabling SSL authentication means explicitly adding certificates of all clients and peers to a list of trusted certificates of each Apache Cassandra server.

To start, we must make sure all of our server nodes can talk to each other using SSL authentication. For that, we'll use the following ruby script:

```ruby
servers = [
  {
    :alias           => 'node1',
    :keystore        => 'node1/conf/.keystore',
    :kestore_pass    => "some very long and secure password",
    :truststore      => 'node1/conf/.truststore',
    :truststore_pass => "another very long and secure password"
  },
  {
    :alias           => 'node2',
    :keystore        => 'node2/conf/.keystore',
    :kestore_pass    => "some very long and secure password",
    :truststore      => 'node2/conf/.truststore',
    :truststore_pass => "another very long and secure password"
  },
  {
    :alias           => 'node3',
    :keystore        => 'node3/conf/.keystore',
    :kestore_pass    => "some very long and secure password",
    :truststore      => 'node3/conf/.truststore',
    :truststore_pass => "another very long and secure password"
  },
]

# we'll iterate over each server and add all other server certificates it its truststore
servers.each do |server|
  truststore = server[:truststore]
  storepass  = server[:truststore_pass]

  servers.each do |peer|
    next if peer == server # skip self

    peer_alias     = peer[:alias]
    peer_keystore  = peer[:keystore]
    peer_storepass = peer[:keystore_pass]

    # export .der certificate from this peer's keystore if we haven't already
    unless File.exists?("#{peer_alias}.crt")
      system("keytool -exportcert -rfc -alias \"#{peer_alias}\" -keystore \"#{peer_keystore}\" -storepass \"#{peer_storepass}\" -file \"#{peer_alias}.crt\"")
    end

    # now we can import extracted peer's DER certificate into server's truststore
    system("keytool -import -noprompt -alias \"#{peer_alias}\" -keystore \"#{truststore}\" -storepass \"#{storepass}\" -file \"#{peer_alias}.crt\"")
  end
end
```

Make sure that all the data in the above script is correct - paths to keystores and truststores as well as passwords and aliases. Save this file to `generate_truststores.rb` and run it with:

```bash
ruby generate_truststores.rb
```

This ensures all our servers are trusting each other. But we're not done yet, it is now time to create a certificate for our client and add it to the servers' truststores.

First, let's create a new keystore for the Ruby Driver (make sure to change the data in the example):

```bash
cnode_alias=driver
cstore_pass="some very long and secure password"

keytool -genkeypair -noprompt \
  -keyalg RSA
  -validity 36500 \
  -alias "$cnode_alias" \
  -keystore "$cnode_alias.keystore" \
  -storepass "$cstore_pass" \
  -dname "CN=Ruby Driver, OU=Ruby Driver Tests, O=DataStax Inc., L=Santa Clara, ST=California, C=US"
```

Check that a file called `driver.keystore` (or whatever the value of `-keystore` option was) exists. Let's export its certificate:

```bash
# values same as above
cnode_alias=driver
cstore_pass="some very long and secure password"

keytool -exportcert -noprompt \
  -alias "$cnode_alias" \
  -keystore "$cnode_alias.keystore" \
  -storepass "$cstore_pass" \
  -file "$cnode_alias.crt"
```

Time to add our driver DER certificate to the truststores of our servers:

```ruby
servers = [
  {
    :alias           => 'node1',
    :keystore        => 'node1/conf/.keystore',
    :kestore_pass    => "some very long and secure password",
    :truststore      => 'node1/conf/.truststore',
    :truststore_pass => "another very long and secure password"
  },
  {
    :alias           => 'node2',
    :keystore        => 'node2/conf/.keystore',
    :kestore_pass    => "some very long and secure password",
    :truststore      => 'node2/conf/.truststore',
    :truststore_pass => "another very long and secure password"
  },
  {
    :alias           => 'node3',
    :keystore        => 'node3/conf/.keystore',
    :kestore_pass    => "some very long and secure password",
    :truststore      => 'node3/conf/.truststore',
    :truststore_pass => "another very long and secure password"
  },
]

driver_cert  = "driver.der"

servers.each do |server|
  truststore = server[:truststore]
  storepass  = server[:truststore_pass]
  alias      = server[:alias]

  system("keytool -import -noprompt -alias \"#{alias}\" -keystore \"#{truststore}\" -storepass \"#{storepass}\" -file \"#{driver_cert}\"")
end
```

Save the above script as `add_to_truststores.rb` and run it with:

```bash
ruby add_to_truststores.rb
```

At this point, we've added the driver's identity to the truststores of all members of Apache Cassandra cluster. Now let's extract a PEM certificate to use with the driver:

```bash
cnode_alias=driver
cstore_pass="some very long and secure password"

keytool -exportcert -noprompt \
  -rfc \
  -alias "$cnode_alias" \
  -keystore "$cnode_alias.keystore" \
  -storepass "$cstore_pass" \
  -file "$cnode_alias.pem"

chmod 400 "$cnode_alias.pem"
```

At this point we have our client PEM key, or a public key. To communicate securely, we'll also need its private key, it will be used to encrypt all communication. We'll covert our java keystore to a format that openssl understands (pkcs12) and use it to extract a private key.

```bash
cnode_alias=driver
cstore_pass="some very long and secure password"
cpassphrase="some secure passphrase for the private key"

keytool -importkeystore -noprompt \
  -srcalias certificatekey \
  -deststoretype PKCS12 \
  -srcalias "$cnode_alias" \
  -srckeystore "$cnode_alias.keystore" \
  -srcstorepass "$cstore_pass" \
  -storepass "$cstore_pass" \
  -destkeystore "$cnode_alias-keystore.p12"

openssl pkcs12 -nomacver -nocerts \
  -in "$cnode_alias-keystore.p12" \
  -password pass:"$cstore_pass" \
  -passout pass:"$cpassphrase" \
  -out "$cnode_alias.key"

chmod 400 "$cnode_alias.key"
```

With all of this ready, let's enable SSL authentication. For that, we should add the following to `cassandra.yaml` on each server:

```yaml
client_encryption_options:
  enabled: true
  keystore: conf/.keystore
  keystore_password: "some very long and secure password"
  require_client_auth: true
  truststore: conf/.truststore
  truststore_password: "another very long and secure password"
```

Make sure to update `cassandra.yaml` on each server with correct data from previous steps.

Finally, we can use our client certificate and key to connect to our Apache Cassandra cluster:

```ruby
cluster = Cassandra.cluster(
  server_cert:  '/path/to/server.pem',
  client_cert:  '/path/to/driver.pem',
  private_key:  '/path/to/driver.key',
  passphrase:   'the passphrase you picked for the key'
)
```

This concludes our overview of SSL encryption with Apache Cassandra. You can [find additional information in cassandra documentation](http://www.datastax.com/documentation/cassandra/2.0/cassandra/security/secureSslEncryptionTOC.html).
