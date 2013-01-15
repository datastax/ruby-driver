# encoding: ascii-8bit

require 'spec_helper'


module Cql
  describe RequestFrame do
    context 'with OPTIONS requests' do
      it 'encodes an OPTIONS request' do
        bytes = RequestFrame.new(OptionsRequest.new).write('')
        bytes.should == "\x01\x00\x00\x05\x00\x00\x00\x00"
      end
    end

    context 'with STARTUP requests' do
      it 'encodes the request' do
        bytes = RequestFrame.new(StartupRequest.new('3.0.0', 'snappy')).write('')
        bytes.should == "\x01\x00\x00\x01\x00\x00\x00\x2b\x00\x02\x00\x0bCQL_VERSION\x00\x053.0.0\x00\x0bCOMPRESSION\x00\x06snappy"
      end

      it 'defaults to CQL 3.0.0 and no compression' do
        bytes = RequestFrame.new(StartupRequest.new()).write('')
        bytes.should == "\x01\x00\x00\x01\x00\x00\x00\x16\x00\x01\x00\x0bCQL_VERSION\x00\x053.0.0"
      end
    end

    context 'with REGISTER requests' do
      it 'encodes the request' do
        bytes = RequestFrame.new(RegisterRequest.new('TOPOLOGY_CHANGE', 'STATUS_CHANGE')).write('')
        bytes.should == "\x01\x00\x00\x0b\x00\x00\x00\x22\x00\x02\x00\x0fTOPOLOGY_CHANGE\x00\x0dSTATUS_CHANGE"
      end
    end

    context 'with QUERY requests' do
      it 'encodes the request' do
        bytes = RequestFrame.new(QueryRequest.new('USE system', :all)).write('')
        bytes.should == "\x01\x00\x00\x07\x00\x00\x00\x10\x00\x00\x00\x0aUSE system\x00\x05"
      end
    end

    context 'with PREPARE requests' do
      it 'encodes the request' do
        bytes = RequestFrame.new(PrepareRequest.new('UPDATE users SET email = ? WHERE user_name = ?')).write('')
        bytes.should == "\x01\x00\x00\x09\x00\x00\x00\x32\x00\x00\x00\x2eUPDATE users SET email = ? WHERE user_name = ?"
      end
    end

    context 'with a stream ID' do
      it 'encodes the stream ID in the header' do
        bytes = RequestFrame.new(QueryRequest.new('USE system', :all), 42).write('')
        bytes[2].should == "\x2a"
      end

      it 'defaults to zero' do
        bytes = RequestFrame.new(QueryRequest.new('USE system', :all)).write('')
        bytes[2].should == "\x00"
      end

      it 'raises an exception if the stream ID is outside of 0..127' do
        expect { RequestFrame.new(QueryRequest.new('USE system', :all), -1) }.to raise_error(InvalidStreamIdError)
        expect { RequestFrame.new(QueryRequest.new('USE system', :all), 128) }.to raise_error(InvalidStreamIdError)
        expect { RequestFrame.new(QueryRequest.new('USE system', :all), 99999999) }.to raise_error(InvalidStreamIdError)
      end
    end
  end
end