# Security

## Authentication

Out of the box, Ruby driver supports [Cassandra's internal authentication mechanism](http://www.datastax.com/documentation/cassandra/2.0/cassandra/security/security_config_native_authenticate_t.html). It is also possible to provide a custom authenticator implementation, refer to `Cassandra::Auth` module for more information.

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
server_alias=node1
keystore_file=conf/.keystore
keystore_pass="some very long and secure password"

CN="Cassandra Node 1"
OU="Drivers and Tools"
O="DataStax Inc."
L="Santa Clara"
ST="California"
C="US"

keytool -genkey -keyalg RSA -noprompt \
  -alias "$server_alias" \
  -keystore "$keystore_file" \
  -storepass "$keystore_pass" \
  -dname "CN=$CN, OU=$OU, O=$O, L=$L, ST=$ST, C=$C"
```

Once we've run the script above for all nodes in our cluster, we can configure our Apache Cassandra servers to use their respective `.keystore`s. For this, we'll have to modify `cassandra.yaml` to include the following:

```yaml
client_encryption_options:
  enabled: true
  keystore: conf/.keystore
  keystore_password: "some very long and secure password"
```

__Note__: The values of `keystore` and `keystore_password` above must be the same as the values of `$keystore_file` and `$keystore_pass` from our shell script.

Now you can restart your cassandra processes.

At this point you already have SSL enabled and can even connect to the servers using DataStax Ruby Driver:

```ruby
require 'cassandra'

cluster = Cassandra.cluster(ssl: true)
```

This however is like having no security at all since the driver won't be able to verify the identity of the server.

#### Extracting server certificate for peer verification

There are several ways to have client verify server's identity. I'm going to extract a PEM certificate of the server, which is suitable for use with the OpenSSL library that the Ruby Driver uses, and give it to the client for verification.

First, we must export a DER certificate of the server:

```bash
# values same as above
server_alias=node1
keystore_file=conf/.keystore
keystore_pass="some very long and secure password"

keytool -export \
  -alias "$server_alias" \
  -keystore "$keystore_file" \
  -storepass "$keystore_pass" \
  -file "$server_alias.der"
```

__Note__: The values of `$server_alias`, `$keystore_file` and `$keystore_pass` must be the same as in the script that we used to generate the keystore file.

Now that we have our DER certificate, we can use OpenSSL to transform it into a PEM file:

```bash
openssl x509 -out "$server_alias.pem" -outform pem -in "$server_alias.der" -inform der
```

This created a PEM certificate out of our DER source certificate that we extracted from the keystore. This process has to be repeated for each unique keystore that we created in the very first section of this guide.

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
    unless File.exists?("#{peer_alias}.der")
      system("keytool -export -alias \"#{peer_alias}\" -keystore \"#{peer_keystore}\" -storepass \"#{peer_storepass}\" -file \"#{peer_alias}.der\"")
    end

    # now we can import extracted peer's DER certificate into server's truststore
    system("keytool -import -noprompt -alias \"#{peer_alias}\" -keystore \"#{truststore}\" -storepass \"#{storepass}\" -file \"#{peer_alias}.der\"")
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
driver_alias=driver
keystore_file=driver.keystore
keystore_pass="some very long and secure password"

CN="Ruby Driver"
OU="Drivers and Tools"
O="DataStax Inc."
L="Santa Clara"
ST="California"
C="US"

keytool -genkey -keyalg RSA -noprompt \
  -alias "$driver_alias" \
  -keystore "$keystore_file" \
  -storepass "$keystore_pass" \
  -dname "CN=$CN, OU=$OU, O=$O, L=$L, ST=$ST, C=$C"
```

Check that a file called `driver.keystore` (or whatever the value of `$keystore_file` was) exists. Let's export its DER certificate:

```bash
# values same as above
driver_alias=driver
keystore_file=driver.keystore
keystore_pass="some very long and secure password"

keytool -export \
  -alias "$driver_alias" \
  -keystore "$keystore_file" \
  -storepass "$keystore_pass" \
  -file "$server_alias.der"
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

driver_alias = "driver"
driver_cert  = "driver.pem"

servers.each do |server|
  truststore = server[:truststore]
  storepass  = server[:truststore_pass]

  system("keytool -import -noprompt -alias \"#{driver_alias}\" -keystore \"#{truststore}\" -storepass \"#{storepass}\" -file \"#{driver_cert}\"")
end
```

Save the above script as `add_to_truststores.rb` and run it with:

```bash
ruby add_to_truststores.rb
```

At this point, we've added the driver's identity to the truststores of all members of Apache Cassandra cluster. Now we will use `openssl` to convert the exported DER certificate to a PEM one:

```bash
openssl x509 -in driver.der -inform der -out driver.pem -outform pem
```

At this point we have our client PEM key, or a public key. To communicate securely, we'll also need its private key, it will be used to encrypt all communication. [We'll have to write some java code to extract this information from `driver.keystore`](http://www.herongyang.com/crypto/Migrating_Keys_keytool_to_OpenSSL_3.html):

```java
/* DumpKey.java
 * Copyright (c) 2007 by Dr. Herong Yang, http://www.herongyang.com/
 */
import java.io.*;
import java.security.*;
public class DumpKey {
   static public void main(String[] a) {
      if (a.length<5) {
         System.out.println("Usage:");
         System.out.println(
            "java DumpKey jks storepass alias keypass out");
         return;
      }
      String jksFile = a[0];
      char[] jksPass = a[1].toCharArray();
      String keyName = a[2];
      char[] keyPass = a[3].toCharArray();
      String outFile = a[4];

      try {
         KeyStore jks = KeyStore.getInstance("jks");
         jks.load(new FileInputStream(jksFile), jksPass);
         Key key = jks.getKey(keyName, keyPass);
         System.out.println("Key algorithm: "+key.getAlgorithm());
         System.out.println("Key format: "+key.getFormat());
         System.out.println("Writing key in binary form to "
            +outFile);

         FileOutputStream out = new FileOutputStream(outFile);
         out.write(key.getEncoded());
         out.close();
      } catch (Exception e) {
         e.printStackTrace();
         return;
      }
   }
}
```

Save the code above to `DumpKey.java`. Let's use it to extract the private key:

```bash
# values same as above
driver_alias=driver
keystore_file=driver.keystore
keystore_pass="some very long and secure password"

javac DumpKey.java
java DumpKey "$keystore_file" "$keystore_pass" "$driver_alias" "$keystore_pass" driver_bin.key
```

This should drop the contents of `driver.keystore` private key into `driver_bin.key`. [We must now add PEM standard header and footer](http://www.herongyang.com/crypto/Migrating_Keys_keytool_to_OpenSSL_4.html).

```bash
# prepend PEM header
echo "-----BEGIN PRIVATE KEY-----" | cat - driver_bin.key > driver.key
# append PEM footer
echo "-----END PRIVATE KEY-----" >> driver.key
```

One more thing before we're ready - let's set up a passphrase for our private key, to make sure only we and our application can use it:

```bash
ssh-keygen -p -f driver.key
```

The above script will prompt you to enter a secure passphrase for the key. Make sure to remember it as we'll use it below.

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
