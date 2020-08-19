# encoding: utf-8

#--
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

require 'spec_helper'

module Cassandra
  describe Types::Duration do
    describe "#new for type" do

      it "should fail if it doesn't get three values" do
        expect { Types.duration.new 1,2 }.to raise_error(ArgumentError)
        expect { Types.duration.new 1,2,3,4 }.to raise_error(ArgumentError)
      end

      it "should fail if it gets a non-integer value" do
        expect { Types.duration.new 1,2,"3" }.to raise_error(ArgumentError)
      end

      it "should fail if it gets values which aren't sized correctly" do
        expect { Types.duration.new 4294967296,2,3 }.to raise_error(ArgumentError)
        expect { Types.duration.new 1,4294967296,3 }.to raise_error(ArgumentError)

        # zero should be fine for all, though
        Types.duration.new 0,2,3
        Types.duration.new 1,0,3
        Types.duration.new 1,2,0
      end

      it "should fail if it gets values which aren't of uniform sign" do
        expect { Types.duration.new -1,2,3 }.to raise_error(ArgumentError)
        expect { Types.duration.new 1,-2,3 }.to raise_error(ArgumentError)
        expect { Types.duration.new 1,2,-3 }.to raise_error(ArgumentError)

        expect { Types.duration.new -1,-2,3 }.to raise_error(ArgumentError)
        expect { Types.duration.new 1,-2,-3 }.to raise_error(ArgumentError)
        expect { Types.duration.new -1,2,-3 }.to raise_error(ArgumentError)
      end
    end
  end
end
