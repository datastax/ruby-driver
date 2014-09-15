// Copyright 2013-2014 DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import java.io.IOException;
        
import org.jruby.Ruby;
import org.jruby.RubyString;
import org.jruby.RubyFixnum;
import org.jruby.RubyModule;
import org.jruby.anno.JRubyModule;
import org.jruby.anno.JRubyMethod;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

public class CassandraMurmur3Service implements BasicLibraryService
{
    public boolean basicLoad(final Ruby runtime) throws IOException
    {
        RubyModule cassandra = runtime.defineModule("Cassandra");
        RubyModule murmur3   = runtime.defineModuleUnder("Murmur3", cassandra);

        murmur3.defineAnnotatedMethods(Murmur3.class);

        return true;
    }

    @JRubyModule(name="Murmur3")
    public static class Murmur3
    {
        // This is an adapted version of the MurmurHash.hash3_x64_128 from
        // Cassandra used for M3P. Compared to that methods, there's a few
        // inlining of arguments and we only return the first 64-bits of the
        // result since that's all M3P uses.
        @JRubyMethod(name="hash", module=true)
        public static RubyFixnum hash(ThreadContext context, IRubyObject self, IRubyObject object)
        {
            byte[] bytes = object.convertToString().getBytes();
            int length   = bytes.length;

            final int nblocks = length >> 4; // Process as 128-bit blocks.

            long h1 = 0;
            long h2 = 0;

            long c1 = 0x87c37b91114253d5L;
            long c2 = 0x4cf5ad432745937fL;

            //----------
            // body

            for(int i = 0; i < nblocks; i++)
            {
                long k1 = getblock(bytes, i * 2 + 0);
                long k2 = getblock(bytes, i * 2 + 1);

                k1 *= c1;
                k1  = rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;

                h1  = rotl64(h1, 27);
                h1 += h2;
                h1  = h1 * 5 + 0x52dce729;

                k2 *= c2;
                k2  = rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;

                h2  = rotl64(h2, 31);
                h2 += h1;
                h2  = h2 * 5 + 0x38495ab5;
            }

            //----------
            // tail

            // Set offset to the unprocessed tail of the data.
            int offset = nblocks * 16;

            long k1 = 0;
            long k2 = 0;

            switch(length & 15)
            {
                case 15: k2 ^= ((long) bytes[offset + 14]) << 48;
                case 14: k2 ^= ((long) bytes[offset + 13]) << 40;
                case 13: k2 ^= ((long) bytes[offset + 12]) << 32;
                case 12: k2 ^= ((long) bytes[offset + 11]) << 24;
                case 11: k2 ^= ((long) bytes[offset + 10]) << 16;
                case 10: k2 ^= ((long) bytes[offset +  9]) << 8;
                case  9: k2 ^= ((long) bytes[offset +  8]) << 0;
                         k2 *= c2;
                         k2  = rotl64(k2, 33);
                         k2 *= c1;
                         h2 ^= k2;
                case  8: k1 ^= ((long) bytes[offset + 7]) << 56;
                case  7: k1 ^= ((long) bytes[offset + 6]) << 48;
                case  6: k1 ^= ((long) bytes[offset + 5]) << 40;
                case  5: k1 ^= ((long) bytes[offset + 4]) << 32;
                case  4: k1 ^= ((long) bytes[offset + 3]) << 24;
                case  3: k1 ^= ((long) bytes[offset + 2]) << 16;
                case  2: k1 ^= ((long) bytes[offset + 1]) << 8;
                case  1: k1 ^= ((long) bytes[offset]);
                         k1 *= c1;
                         k1  = rotl64(k1, 31);
                         k1 *= c2;
                         h1 ^= k1;
            };

            //----------
            // finalization

            h1 ^= length;
            h2 ^= length;

            h1 += h2;
            h2 += h1;

            h1 = fmix(h1);
            h2 = fmix(h2);

            h1 += h2;
            h2 += h1;

            return RubyFixnum.newFixnum(context.runtime, h1);
        }

        protected static long getblock(byte[] bytes, int index)
        {
            int offset = index << 3;
            return ((long) bytes[offset + 0] & 0xff) +
                  (((long) bytes[offset + 1] & 0xff) << 8) +
                  (((long) bytes[offset + 2] & 0xff) << 16) +
                  (((long) bytes[offset + 3] & 0xff) << 24) +
                  (((long) bytes[offset + 4] & 0xff) << 32) +
                  (((long) bytes[offset + 5] & 0xff) << 40) +
                  (((long) bytes[offset + 6] & 0xff) << 48) +
                  (((long) bytes[offset + 7] & 0xff) << 56)
            ;
        }

        protected static long rotl64(long v, int n)
        {
            return ((v << n) | (v >>> (64 - n)));
        }

        protected static long fmix(long k)
        {
            k ^= k >>> 33;
            k *= 0xff51afd7ed558ccdL;
            k ^= k >>> 33;
            k *= 0xc4ceb9fe1a85ec53L;
            k ^= k >>> 33;

            return k;
        }
    }
}
