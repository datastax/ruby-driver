# encoding: ascii-8bit

require 'spec_helper'


module Cql
  module Protocol
    describe PreparedResultResponse do
      describe '.decode!' do
        let :response do
          described_class.decode!(1, ByteBuffer.new("\x00\x10\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/\x00\x00\x00\x01\x00\x00\x00\x01\x00\ncql_rb_911\x00\x05users\x00\tuser_name\x00\r"))
        end

        it 'decodes the ID' do
          response.id.should == "\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/"
        end

        it 'decodes the column metadata' do
          response.metadata.should == [['cql_rb_911', 'users', 'user_name', :varchar]]
        end
      end

      describe '#void?' do
        it 'is not void' do
          response = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          response.should_not be_void
        end
      end

      describe '#to_s' do
        it 'returns a string with the ID and metadata' do
          response = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\x00/", [['ks', 'tbl', 'col', :varchar]], nil)
          response.to_s.should match(/^RESULT PREPARED [0-9a-f]{32} \[\["ks", "tbl", "col", :varchar\]\]$/)
        end
      end

      describe '#eql?' do
        it 'is equal to an identical response' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r1.should eql(r2)
        end

        it 'is not equal when the IDs differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r2 = described_class.new("\x00" * 16, [['ks', 'tbl', 'col', :varchar]], nil)
          r1.should_not eql(r2)
        end

        it 'is not equal when the metadata differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar], ['ks', 'tbl', 'col2', :uuid]], nil)
          r1.should_not eql(r2)
        end

        it 'is not equal when one has a trace ID' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r1.should_not eql(r2)
        end

        it 'is not equal when the trace IDs differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], Uuid.new('11111111-d0e1-11e2-8b8b-0800200c9a66'))
          r1.should_not eql(r2)
        end

        it 'is aliased as ==' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r1.should == r2
        end
      end

      describe '#hash' do
        it 'is the same for an identical response' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r1.hash.should == r2.hash
        end

        it 'is not the same when the IDs differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r2 = described_class.new("\x00" * 16, [['ks', 'tbl', 'col', :varchar]], nil)
          r1.hash.should_not == r2.hash
        end

        it 'is not the same when the metadata differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar], ['ks', 'tbl', 'col2', :uuid]], nil)
          r1.hash.should_not == r2.hash
        end

        it 'is not the same when one has a trace ID' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], nil)
          r1.hash.should_not == r2.hash
        end

        it 'is not equal when the trace IDs differ' do
          r1 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], Uuid.new('00b69180-d0e1-11e2-8b8b-0800200c9a66'))
          r2 = described_class.new("\xCAH\x7F\x1Ez\x82\xD2<N\x8A\xF35Qq\xA5/", [['ks', 'tbl', 'col', :varchar]], Uuid.new('11111111-d0e1-11e2-8b8b-0800200c9a66'))
          r1.hash.should_not == r2.hash
        end
      end
    end
  end
end
