# encoding: utf-8

RSpec::Matchers.define :eql_bytes do |expected|
  match do |actual|
    actual.unpack('c*') == expected.unpack('c*')
  end
end