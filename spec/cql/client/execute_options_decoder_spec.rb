# encoding: utf-8

require 'spec_helper'


module Cql
  module Client
    describe ExecuteOptionsDecoder do
      let :decoder do
        described_class.new(:two)
      end

      describe '#decode_options' do
        it 'returns the default consistency' do
          options = decoder.decode_options({})
          options.should include(consistency: :two)
        end

        it 'returns the default consistency when given nil' do
          options = decoder.decode_options(nil)
          options.should include(consistency: :two)
        end

        it 'uses the consistency given in the options' do
          options = decoder.decode_options(consistency: :three)
          options.should include(consistency: :three)
        end

        it 'uses the consistency given as a symbol' do
          options = decoder.decode_options(:three)
          options.should include(consistency: :three)
        end

        it 'defaults to no tracing' do
          options = decoder.decode_options({})
          options.should_not have_key(:trace)
        end

        it 'uses the tracing value given in the options' do
          options = decoder.decode_options(trace: true)
          options.should include(trace: true)
          options = decoder.decode_options(trace: false)
          options.should include(trace: false)
        end

        it 'defaults to no timeout' do
          options = decoder.decode_options({})
          options.should_not have_key(:timeout)
        end

        it 'uses the timeout value given in the options' do
          options = decoder.decode_options(timeout: 3)
          options.should include(timeout: 3)
        end
      end
    end
  end
end