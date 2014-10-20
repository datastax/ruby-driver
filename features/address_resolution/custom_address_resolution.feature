Feature: Custom address resolution policy

  Ruby driver uses plug-able address-resolution policy to map Apache Cassandra
  node's ip address to another address value if necessary.

  @wip
  Scenario: Using a custom address resolution strategy
    Given a running cassandra cluster in 2 datacenters with 2 nodes in each
    And a file named "custom_address_resolver.rb" with:
      """ruby
      class CustomAddressResolver
        def initialize(addresses)
          @addresses = addresses
        end

        def resolve(address)
          @addresses.fetch(address, address)
        end
      end
      """
    And the following example:
      """ruby
      require 'cassandra'
      require 'custom_address_resolver'

      resolver  = CustomAddressResolver.new({
        IPAddr.new('127.0.0.3') => IPAddr.new('192.168.10.3'),
        IPAddr.new('127.0.0.4') => IPAddr.new('192.168.10.4')
      })
      cluster   = Cassandra.cluster(address_resolution_policy: resolver)

      puts cluster.each_host.map!(&:ip).sort!
      """
    When it is executed
    Then its output should contain:
      """
      127.0.0.1
      127.0.0.2
      192.168.10.3
      192.168.10.4
      """
