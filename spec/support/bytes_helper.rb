# encoding: utf-8

RSpec::Matchers.define :eql_bytes do |expected|
  match do |actual|
    actual.to_s.unpack('c*') == expected.to_s.unpack('c*')
  end
end