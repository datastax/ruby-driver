require 'spec_helper'
require 'tempfile'

# Create a short-hand alias to more easily get to the validate_and_massage_options
# method in the tests.
# noinspection RubyConstantNamingConvention
C = Cassandra
# noinspection RubyClassModuleNamingConvention
module C
  class << self
    # noinspection RubyResolve
    alias :validate :validate_and_massage_options
  end
end

describe Cassandra do
  context :validate_and_massage_options do
    it 'should ignore spurious options' do
      expect(C.validate(foo: 1, timeout: 5)).to eq({ timeout: 5 })
    end

    context 'for username and password' do
      it 'should require both or none of username and password' do
        # None
        expect(C.validate({})).to eq({})

        # Both
        actual = C.validate(username: 'u1', password: 'p1')
        expect(actual[:credentials]).to eq({ username: 'u1', password: 'p1' })

        # One and not the other.
        expect { C.validate(username: 'u1') }.to raise_error(ArgumentError)
        expect { C.validate(password: 'p1') }.to raise_error(ArgumentError)
      end

      it 'should reject empty/nil/non-String username' do
        expect { C.validate(username: '', password: 'p1') }.
            to raise_error(ArgumentError)
        expect { C.validate(username: nil, password: 'p1') }.
            to raise_error(ArgumentError)
        expect { C.validate(username: 7, password: 'p1') }.
            to raise_error(ArgumentError)
      end

      it 'should reject empty/nil/non-String password' do
        expect { C.validate(username: 'u1', password: '') }.
            to raise_error(ArgumentError)
        expect { C.validate(username: 'u1', password: nil) }.
            to raise_error(ArgumentError)
        expect { C.validate(username: 'u1', password: 7) }.
            to raise_error(ArgumentError)
      end
    end

    context 'for ssl' do
      before do
        @client_cert_file = Tempfile.new('testclicert')
        @key_file = Tempfile.new('testkey')
        @server_cert_file = Tempfile.new('testsvrcert')
      end

      after do
        @client_cert_file.unlink
        @key_file.unlink
        @server_cert_file.unlink
      end

      it 'should require both or none of :client_cert and :private_key' do
        # None
        expect(C.validate({})).to eq({})

        # Both; since we don't have a valid cert / key, this will fail with
        # a CertificateError.
        # TODO: Make the client cert and key files legit.
        expect {
          C.validate(client_cert: @client_cert_file.path, private_key: @key_file.path)
        }.to raise_error(OpenSSL::X509::CertificateError)

        # One and not the other.
        expect { C.validate(client_cert: @client_cert_file.path) }.
            to raise_error(ArgumentError)
        expect { C.validate(private_key: @key_file.path) }.
            to raise_error(ArgumentError)
      end

      it 'should validate that :client_cert and :private_key files exist' do
        expect {
          C.validate(client_cert: 'noexist1', private_key: @key_file.path)
        }.to raise_error(ArgumentError)
        expect {
          C.validate(client_cert: @client_cert_file.path, private_key: 'noexist1')
        }.to raise_error(ArgumentError)
        expect {
          C.validate(client_cert: @client_cert_file.path, private_key: nil)
        }.to raise_error(ArgumentError)
        expect {
          C.validate(client_cert: nil, private_key: @key_file.path)
        }.to raise_error(ArgumentError)
      end

      it 'should validate that :server_cert file exists' do
        expect { C.validate(server_cert: @server_cert_file.path) }.to_not raise_error
        expect { C.validate(server_cert: 'noexist1') }.to raise_error(ArgumentError)
        expect { C.validate(server_cert: nil) }.to raise_error(ArgumentError)
      end

      it 'should validate :ssl option' do
        expect { C.validate(ssl: true) }.to_not raise_error
        expect { C.validate(ssl: false) }.to_not raise_error
        expect { C.validate(ssl: ::OpenSSL::SSL::SSLContext.new) }.to_not raise_error
        expect { C.validate(ssl: 7) }.to raise_error(ArgumentError)
        expect { C.validate(ssl: 'abc') }.to raise_error(ArgumentError)
        expect { C.validate(ssl: nil) }.to raise_error(ArgumentError)
      end
    end

    it 'should massage :compression option' do
      expect(C.validate(compression: :snappy)[:compressor].class).
          to be(C::Compression::Compressors::Snappy)
      expect(C.validate(compression: :lz4)[:compressor].class).
          to be(C::Compression::Compressors::Lz4)
      expect { C.validate(compression: 'junk') }.to raise_error(ArgumentError)
      expect { C.validate(compression: nil) }.to raise_error(ArgumentError)
    end

    it 'should validate :compressor option' do
      class GoodCompressor
        def algorithm
        end

        def compress?
        end

        def compress
        end

        def decompress
        end
      end
      compressor = GoodCompressor.new
      expect(C.validate(compressor: compressor)).to eq({ compressor: compressor })
      expect { C.validate(compressor: 'junk') }.to raise_error(ArgumentError)
      expect { C.validate(compressor: nil) }.to raise_error(ArgumentError)
    end

    it 'should validate :logger option' do
      logger = C::NullLogger.new
      expect(C.validate(logger: logger)).to eq({ logger: logger })
      expect(C.validate(logger: nil)).to eq({})
      expect { C.validate(logger: 'junk') }.to raise_error(ArgumentError)
    end

    it 'should validate :port' do
      expect { C.validate(port: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(port: 0) }.to raise_error(ArgumentError)
      expect { C.validate(port: 65536) }.to raise_error(ArgumentError)
      expect { C.validate(port: 1.5) }.to raise_error(ArgumentError)
      expect(C.validate(port: 1234)).to eq({ port: 1234 })
      expect(C.validate(port: 1)).to eq({ port: 1 })
      expect(C.validate(port: 65535)).to eq({ port: 65535 })
      expect(C.validate(port: nil)).to eq({ port: nil })
    end

    it 'should validate :connect_timeout' do
      expect { C.validate(connect_timeout: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(connect_timeout: -1) }.to raise_error(ArgumentError)
      expect { C.validate(connect_timeout: 0) }.to raise_error(ArgumentError)
      expect(C.validate(connect_timeout: 0.5)).to eq({ connect_timeout: 0.5 })
      expect(C.validate(connect_timeout: 38)).to eq({ connect_timeout: 38 })
      expect(C.validate(connect_timeout: nil)).to eq({ connect_timeout: nil })
    end

    it 'should validate :timeout' do
      expect { C.validate(timeout: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(timeout: -1) }.to raise_error(ArgumentError)
      expect { C.validate(timeout: 0) }.to raise_error(ArgumentError)
      expect(C.validate(timeout: 0.5)).to eq({ timeout: 0.5 })
      expect(C.validate(timeout: 38)).to eq({ timeout: 38 })
      expect(C.validate(timeout: nil)).to eq({ timeout: nil })
    end

    it 'should validate :heartbeat_interval' do
      expect { C.validate(heartbeat_interval: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(heartbeat_interval: -1) }.to raise_error(ArgumentError)
      expect { C.validate(heartbeat_interval: 0) }.to raise_error(ArgumentError)
      expect(C.validate(heartbeat_interval: 0.5)).to eq({ heartbeat_interval: 0.5 })
      expect(C.validate(heartbeat_interval: 38)).to eq({ heartbeat_interval: 38 })
      expect(C.validate(heartbeat_interval: nil)).to eq({ heartbeat_interval: nil })
    end

    it 'should validate :idle_timeout' do
      expect { C.validate(idle_timeout: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(idle_timeout: -1) }.to raise_error(ArgumentError)
      expect { C.validate(idle_timeout: 0) }.to raise_error(ArgumentError)
      expect(C.validate(idle_timeout: 0.5)).to eq({ idle_timeout: 0.5 })
      expect(C.validate(idle_timeout: 38)).to eq({ idle_timeout: 38 })
      expect(C.validate(idle_timeout: nil)).to eq({ idle_timeout: nil })
    end

    it 'should validate :schema_refresh_delay' do
      expect { C.validate(schema_refresh_delay: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(schema_refresh_delay: -1) }.to raise_error(ArgumentError)
      expect { C.validate(schema_refresh_delay: 0) }.to raise_error(ArgumentError)
      expect {C.validate(schema_refresh_delay: nil)}.to raise_error(ArgumentError)
      expect(C.validate(schema_refresh_delay: 0.5)).to eq({ schema_refresh_delay: 0.5 })
      expect(C.validate(schema_refresh_delay: 38)).to eq({ schema_refresh_delay: 38 })
    end

    it 'should validate :schema_refresh_timeout' do
      expect { C.validate(schema_refresh_timeout: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(schema_refresh_timeout: -1) }.to raise_error(ArgumentError)
      expect { C.validate(schema_refresh_timeout: 0) }.to raise_error(ArgumentError)
      expect {C.validate(schema_refresh_timeout: nil)}.to raise_error(ArgumentError)
      expect(C.validate(schema_refresh_timeout: 0.5)).to eq({ schema_refresh_timeout: 0.5 })
      expect(C.validate(schema_refresh_timeout: 38)).to eq({ schema_refresh_timeout: 38 })
    end

    it 'should validate :load_balancing_policy option' do
      class GoodPolicy
        def host_up
        end

        def host_down
        end

        def host_found
        end

        def host_lost
        end

        def setup
        end

        def teardown
        end

        def distance
        end

        def plan
        end
      end
      policy = GoodPolicy.new
      expect(C.validate(load_balancing_policy: policy)).to eq({ load_balancing_policy: policy })
      expect { C.validate(load_balancing_policy: 'junk') }.to raise_error(ArgumentError)
      expect { C.validate(load_balancing_policy: nil) }.to raise_error(ArgumentError)
    end

    it 'should validate :reconnection_policy option' do
      class GoodPolicy
        def schedule
        end
      end
      policy = GoodPolicy.new
      expect(C.validate(reconnection_policy: policy)).to eq({ reconnection_policy: policy })
      expect { C.validate(reconnection_policy: 'junk') }.to raise_error(ArgumentError)
      expect { C.validate(reconnection_policy: nil) }.to raise_error(ArgumentError)
    end

    it 'should validate :retry_policy option' do
      class GoodPolicy
        def read_timeout
        end

        def write_timeout
        end

        def unavailable
        end
      end
      policy = GoodPolicy.new
      expect(C.validate(retry_policy: policy)).to eq({ retry_policy: policy })
      expect { C.validate(retry_policy: 'junk') }.to raise_error(ArgumentError)
      expect { C.validate(retry_policy: nil) }.to raise_error(ArgumentError)
    end

    it 'should massage :listeners into an array' do
      expect(C.validate(listeners: 'a')).to eq({ listeners: ['a'] })
      expect(C.validate(listeners: ['a'])).to eq({ listeners: ['a'] })
      expect(C.validate(listeners: nil)).to eq({ listeners: [] })
    end

    it 'should validate :consistency' do
      Cassandra::CONSISTENCIES.each do |c|
        expect(C.validate(consistency: c)).to eq({ consistency: c })
      end
      expect { C.validate(consistency: 'foo') }.to raise_error(ArgumentError)
    end

    it 'should massage :nodelay to a boolean' do
      expect(C.validate(nodelay: nil)).to eq({nodelay: false})
      expect(C.validate(nodelay: 1)).to eq({nodelay: true})
      expect(C.validate(nodelay: 0)).to eq({nodelay: true})
    end

    it 'should massage :trace to a boolean' do
      expect(C.validate(trace: nil)).to eq({trace: false})
      expect(C.validate(trace: 1)).to eq({trace: true})
      expect(C.validate(trace: 0)).to eq({trace: true})
    end

    it 'should massage :shuffle_replicas to a boolean' do
      expect(C.validate(shuffle_replicas: nil)).to eq({shuffle_replicas: false})
      expect(C.validate(shuffle_replicas: 1)).to eq({shuffle_replicas: true})
      expect(C.validate(shuffle_replicas: 0)).to eq({shuffle_replicas: true})
    end

    it 'should validate :page_size' do
      expect { C.validate(page_size: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(page_size: 0) }.to raise_error(ArgumentError)
      expect { C.validate(page_size: 1.5) }.to raise_error(ArgumentError)
      expect { C.validate(page_size: 2**32) }.to raise_error(ArgumentError)
      expect(C.validate(page_size: 1234)).to eq({ page_size: 1234 })
      expect(C.validate(page_size: 1)).to eq({ page_size: 1 })
      expect(C.validate(page_size: 2**32 - 1)).to eq({ page_size: 2**32 - 1 })
      expect(C.validate(page_size: nil)).to eq({ page_size: nil })
    end

    it 'should validate :protocol_version' do
      expect { C.validate(protocol_version: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(protocol_version: 0) }.to raise_error(ArgumentError)
      expect { C.validate(protocol_version: 1.5) }.to raise_error(ArgumentError)
      expect { C.validate(protocol_version: 5) }.to raise_error(ArgumentError)
      expect(C.validate(protocol_version: 1)).to eq({ protocol_version: 1 })
      expect(C.validate(protocol_version: 4)).to eq({ protocol_version: 4 })
      expect(C.validate(protocol_version: nil)).to eq({ protocol_version: nil })
    end

    it 'should validate :futures_factory option' do
      class GoodFactory
        def error
        end
        
        def value
        end
        
        def promise
        end
        
        def all
        end
      end
      factory = GoodFactory.new
      expect(C.validate(futures_factory: factory)).to eq({ futures_factory: factory })
      expect { C.validate(futures_factory: 'junk') }.to raise_error(ArgumentError)
      expect { C.validate(futures_factory: nil) }.to raise_error(ArgumentError)
    end

    it 'should massage :address_resolution option' do
      expect(C.validate(address_resolution: :ec2_multi_region)[:address_resolution_policy].class).
          to be(C::AddressResolution::Policies::EC2MultiRegion)
      expect(C.validate(address_resolution: :none)).to eq({})
      expect { C.validate(address_resolution: 'junk') }.to raise_error(ArgumentError)
      expect { C.validate(address_resolution: nil) }.to raise_error(ArgumentError)
    end

    it 'should validate :address_resolution_policy option' do
      class GoodPolicy
        def resolve
        end
      end
      policy = GoodPolicy.new
      expect(C.validate(address_resolution_policy: policy)).
          to eq({ address_resolution_policy: policy })
      expect { C.validate(address_resolution_policy: 'junk') }.to raise_error(ArgumentError)
      expect { C.validate(address_resolution_policy: nil) }.to raise_error(ArgumentError)
    end

    it 'should massage :synchronize_schema to a boolean' do
      expect(C.validate(synchronize_schema: nil)).to eq({synchronize_schema: false})
      expect(C.validate(synchronize_schema: 1)).to eq({synchronize_schema: true})
      expect(C.validate(synchronize_schema: 0)).to eq({synchronize_schema: true})
    end

    it 'should massage :client_timestamps to a boolean' do
      expect(C.validate(client_timestamps: nil)).to eq({client_timestamps: false})
      expect(C.validate(client_timestamps: 1)).to eq({client_timestamps: true})
      expect(C.validate(client_timestamps: 0)).to eq({client_timestamps: true})
    end

    it 'should validate :timestamp_generator' do
      valid_generator = Object.new
      def valid_generator.next
        42
      end
      expect { C.validate(timestamp_generator: Object.new) }.to raise_error(ArgumentError)
      expect(C.validate(timestamp_generator: valid_generator)).
          to eq({ timestamp_generator: valid_generator })
    end

    it 'should validate :connections_per_local_node' do
      expect { C.validate(connections_per_local_node: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(connections_per_local_node: 0) }.to raise_error(ArgumentError)
      expect { C.validate(connections_per_local_node: 1.5) }.to raise_error(ArgumentError)
      expect { C.validate(connections_per_local_node: 2**16) }.to raise_error(ArgumentError)
      expect(C.validate(connections_per_local_node: 1234)).
          to eq({ connections_per_local_node: 1234 })
      expect(C.validate(connections_per_local_node: 1)).
          to eq({ connections_per_local_node: 1 })
      expect(C.validate(connections_per_local_node: 2**16 - 1)).
          to eq({ connections_per_local_node: 2**16 - 1 })
      expect(C.validate(connections_per_local_node: nil)).
          to eq({ connections_per_local_node: nil })
    end

    it 'should validate :connections_per_remote_node' do
      expect { C.validate(connections_per_remote_node: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(connections_per_remote_node: 0) }.to raise_error(ArgumentError)
      expect { C.validate(connections_per_remote_node: 1.5) }.to raise_error(ArgumentError)
      expect { C.validate(connections_per_remote_node: 2**16) }.to raise_error(ArgumentError)
      expect(C.validate(connections_per_remote_node: 1234)).
          to eq({ connections_per_remote_node: 1234 })
      expect(C.validate(connections_per_remote_node: 1)).
          to eq({ connections_per_remote_node: 1 })
      expect(C.validate(connections_per_remote_node: 2**16 - 1)).
          to eq({ connections_per_remote_node: 2**16 - 1 })
      expect(C.validate(connections_per_remote_node: nil)).
          to eq({ connections_per_remote_node: nil })
    end

    it 'should validate :requests_per_connection' do
      expect { C.validate(requests_per_connection: 'a') }.to raise_error(ArgumentError)
      expect { C.validate(requests_per_connection: 0) }.to raise_error(ArgumentError)
      expect { C.validate(requests_per_connection: 1.5) }.to raise_error(ArgumentError)
      expect { C.validate(requests_per_connection: 2**15) }.to raise_error(ArgumentError)
      expect(C.validate(requests_per_connection: 1234)).
          to eq({ requests_per_connection: 1234 })
      expect(C.validate(requests_per_connection: 1)).
          to eq({ requests_per_connection: 1 })
      expect(C.validate(requests_per_connection: 2**15 - 1)).
          to eq({ requests_per_connection: 2**15 - 1 })
      expect(C.validate(requests_per_connection: nil)).
          to eq({ requests_per_connection: nil })
    end
  end
end
