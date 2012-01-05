/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.memcached;

import org.iq80.memcached.Hash.InternalState;
import org.iq80.memory.Allocation;
import org.iq80.memory.Allocator;
import org.iq80.memory.UnsafeAllocator;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.iq80.memcached.Hash.finalMix;
import static org.iq80.memcached.Hash.hashBigEndian;
import static org.iq80.memcached.Hash.hashLittleEndian;
import static org.iq80.memcached.Hash.mix;
import static org.iq80.memcached.Hash.rot;

@Test
public class HashTest
{
    public void testRot()
    {
        // results generated from a C program
        int[] results = {
                0x00000000,
                0x2468acf0,
                0x48d159e0,
                0x91a2b3c0,

                0x23456781,
                0x468acf02,
                0x8d159e04,
                0x1a2b3c09,

                0x34567812,
                0x68acf024,
                0xd159e048,
                0xa2b3c091,

                0x45678123,
                0x8acf0246,
                0x159e048d,
                0x2b3c091a,

                0x56781234,
                0xacf02468,
                0x59e048d1,
                0xb3c091a2,

                0x67812345,
                0xcf02468a,
                0x9e048d15,
                0x3c091a2b,

                0x78123456,
                0xf02468ac,
                0xe048d159,
                0xc091a2b3,

                0x81234567,
                0x02468acf,
                0x048d159e,
                0x091a2b3c,
        };

        for (int i = 1; i < 32; i++) {
            Assert.assertEquals(rot(0x12345678, i), results[i]);
        }
    }

    public void testMix()
    {
        InternalState[] results = {
                new InternalState(0x8d81d516, 0x05697933, 0xe753edd5),
                new InternalState(0x44efe5ba, 0xb5c7c81d, 0xb953e0b5),
                new InternalState(0x9d8725dd, 0x05a0f784, 0x6f384c10),
                new InternalState(0x11846de8, 0xfe0d5bcd, 0xc241ad85),

                new InternalState(0xc3a8469f, 0x298874e9, 0x8d50ccf3),
                new InternalState(0x0ac7ab4e, 0x7477b967, 0x3349b055),
                new InternalState(0x70d21492, 0x19a73e7a, 0xd09138eb),
                new InternalState(0x590387e6, 0x0dd118e2, 0x0abbf6c7),

                new InternalState(0x9439311f, 0x1b72d17e, 0x5882f771),
                new InternalState(0x3d671348, 0xa9cadae3, 0x72c156c9),
                new InternalState(0x3624b63c, 0x673aadeb, 0xd59c04df),
                new InternalState(0xc7f3c78a, 0x60649cf9, 0x560386c3),

                new InternalState(0x80fa036b, 0x6797e9b7, 0x65186e24),
                new InternalState(0x7349e64c, 0xc3422152, 0x09fe3749),
                new InternalState(0x0353c28d, 0xa9536462, 0x12313580),
                new InternalState(0x14f8771b, 0xc27e8e47, 0xa809a135),

                new InternalState(0xa32efc05, 0x1404f548, 0x55d94f9b),
                new InternalState(0x7950fe06, 0xc82fa08f, 0x92edf97f),
                new InternalState(0xf3c27ab0, 0x734c7451, 0xa920f15e),
                new InternalState(0x59042f44, 0x97a2bad6, 0x506c8d16),

                new InternalState(0xd1d2e1c0, 0xed527974, 0xe770c8c4),
                new InternalState(0x04e6f8ad, 0x6ee8d816, 0x03372a3b),
                new InternalState(0xb8fd51d1, 0x22d2def4, 0xe015ef8c),
                new InternalState(0xe9861d3b, 0x4e9d19b0, 0x12f60cad),

                new InternalState(0x92250857, 0x0d7f4900, 0x1eb131df),
                new InternalState(0xbfb29875, 0x355cc814, 0x1512798a),
                new InternalState(0x4b14a06d, 0xb3e93f2f, 0x7109fbaa),
                new InternalState(0x648fa1d9, 0xde43b430, 0x160f5ffc),

                new InternalState(0xbc65756e, 0xb8d849ca, 0x7b976e85),
                new InternalState(0xde327f8a, 0xda0e0ffd, 0xf6897ba7),
                new InternalState(0x3c6695c7, 0xc764af2b, 0x2ba1e04d),
                new InternalState(0x238ac6fa, 0xcb23ff93, 0x7477770f),
        };

        InternalState state = new InternalState(0x11111111, 0x22222222, 0x33333333);
        for (int i = 0; i < 32; i++) {
            mix(state);
            Assert.assertEquals(state, results[i]);
        }
    }

    public void testFinalMix()
    {
        InternalState[] results = {
                new InternalState(0xbdba5845, 0xdd652d2b, 0x3024eb75),

                new InternalState(0xd348db8a, 0x4fca90fa, 0x1fb6a114),
                new InternalState(0x35d01f3f, 0x802b2a9e, 0x1842b70f),
                new InternalState(0x6d2892aa, 0x9a6c35dc, 0x698471dc),
                new InternalState(0x329313ad, 0xa8831f2e, 0xbae59714),

                new InternalState(0x90b23352, 0x4616dcd9, 0x3734de51),
                new InternalState(0x1306b2d2, 0xee82227f, 0x72dcbbfc),
                new InternalState(0x431f721e, 0x21a53fd9, 0x514a18b4),
                new InternalState(0x5b839f47, 0xe5c17515, 0xf20e3f3e),

                new InternalState(0x28e8eeb2, 0xd34a86b1, 0x9ba443ec),
                new InternalState(0x2953e025, 0x8e182f54, 0x8c2d1e91),
                new InternalState(0x429a3768, 0x35abf999, 0x76599fdc),
                new InternalState(0xdbe6b38c, 0xb29b8806, 0xfa887d8e),

                new InternalState(0xf775ccde, 0x78730ad9, 0x349db268),
                new InternalState(0x994743e0, 0x84d98c5f, 0x71ff79e4),
                new InternalState(0x57680004, 0x3c2f4857, 0x4fd267c7),
                new InternalState(0x33db30a6, 0xc5ccc723, 0x3700d867),

                new InternalState(0xc578d28a, 0xc4e7f646, 0x05600255),
                new InternalState(0xb49b2c9a, 0xce55c473, 0xeb89dfbd),
                new InternalState(0xafd2789d, 0xc550eac9, 0xc2df82bc),
                new InternalState(0x930e3d17, 0xf2bb18c7, 0x279b8351),

                new InternalState(0x6e3549e7, 0x51bc05e2, 0x6b87ad7d),
                new InternalState(0x23e9a09a, 0x065f07a7, 0x9c267231),
                new InternalState(0x91a31fcf, 0xe14ab4aa, 0x0d9cfd7b),
                new InternalState(0x29ae198b, 0xd9da7510, 0x85b83e77),

                new InternalState(0xa978df83, 0x9c1c3bb2, 0x9d782d34),
                new InternalState(0xd660af7a, 0xba52166b, 0x79457bc7),
                new InternalState(0x0390d6b4, 0x974a05d0, 0xe4b9905c),
                new InternalState(0xc7c68736, 0x0cbbd5ee, 0xa8f01815),

                new InternalState(0x1fda03a9, 0x4089ef0d, 0x6e19e6c5),
                new InternalState(0x8fc3ff44, 0x9b210338, 0x1c152e9e),
                new InternalState(0xe38612cf, 0x4be93bbd, 0x4776ed8e),
        };

        InternalState state = new InternalState(0x11111111, 0x22222222, 0x33333333);

        for (int i = 0; i < 32; i++) {
            finalMix(state);
            Assert.assertEquals(state, results[i]);
        }
    }

    public void testHashLittleEndian()
    {
        int[] results = {
                0xdeadbeef,
                0x58d68708,
                0xfbb3a8df,
                0x0e397631,

                0xb5f4889c,
                0x026d72de,
                0xd6fa502e,
                0xb11ad4a5,

                0x2995c3be,
                0xac6572b4,
                0x8bf7d2ef,
                0x5f61edf8,

                0x4012f87b,
                0x928128f9,
                0x2bb84ef8,
                0xa9ce8fb6,

                0x11347272,
                0x8938634e,
                0x1ceaf360,
                0x02a80e47,

                0x372707b2,
                0xdfa3b04b,
                0xa9752892,
                0x4e25bfff,

                0x1b631fea,
                0x6c29c5e2,
                0x7538b5bd,
                0x71b486e3,

                0xbbe9d659,
                0xdf3e4991,
                0xd6863a03,
                0xc100125d,
        };

        byte[] data = "abcdefghijklmnopqrstuvwxyz023456789".getBytes();

        Allocator allocator = UnsafeAllocator.INSTANCE;
        Allocation allocation = allocator.allocate(data.length);
        allocation.putBytes(0, data);

        for (int i = 0; i < 32; i++) {
            Assert.assertEquals(hashLittleEndian(allocation.getRegion(0, i), 0), results[i]);
        }

    }

    @Test(enabled = false)
    public void testHashBigEndian()
    {
        int[] results = {
                0xdeadbeef,
                0xaa6eceb2,
                0x95eef239,
                0x67a65304,

                0xb5f4889c,
                0xa45a7609,
                0xc297d8e2,
                0xa2a71b29,

                0x2995c3be,
                0x43cb7702,
                0x1f508a56,
                0xed1f4b4b,

                0x4012f87b,
                0x1d379627,
                0xafb98514,
                0xdc96da04,

                0x11347272,
                0x1d472031,
                0x23a8d334,
                0xdc480092,

                0x372707b2,
                0xad4ecb21,
                0x381c08f2,
                0x0350b223,

                0x1b631fea,
                0x54b1a8c9,
                0x5abc823d,
                0x8ba22e62,

                0xbbe9d659,
                0x96fd5ab9,
                0x744ec545,
                0x038cda7f,
        };

        byte[] data = "abcdefghijklmnopqrstuvwxyz023456789".getBytes();

        Allocator allocator = UnsafeAllocator.INSTANCE;
        Allocation allocation = allocator.allocate(data.length);
        allocation.putBytes(0, data);

        for (int i = 0; i < 32; i++) {
            Assert.assertEquals(hashBigEndian(allocation.getRegion(0, i), 0), results[i]);
        }

    }


//        int aa = 0xeeeeeeed;
//        int bb = 0x33333335;
//        System.out.printf("b ^ rot(a, 6) -> 0x%08x ^ rot(0x%08x, 6) -> 0x%08x ^ 0x%08x -> 0x%08x\n", bb, aa, bb, rot(aa, 6), bb ^ rot(aa, 6));
//        System.out.printf("b ^ rot(a, 6) -> 0x%08x ^ rot(0x%08x, 6) -> 0x%08x ^ 0x%08x -> 0x%08x\n", bb, aa, bb, rot(aa, 6), bb ^ rot(aa, 6));
//        System.out.printf("\n");
//
//
//        InternalState state = new InternalState(0x11111111, 0x22222222, 0x33333333);
//        System.out.println(state);
//
//        mix1(state);
//        System.out.println(state);
//
////        System.out.printf("0x33333335 ^ 0x444444bb -> 0x%08x\n", (0x33333335 ^ 0x444444bb));
//
//        mix2(state);
//        System.out.println(state);
//
//        mix3(state);
//        System.out.println(state);
//
//        mix4(state);
//        System.out.println(state);
//
//        mix5(state);
//        System.out.println(state);
//
//        mix6(state);
//        System.out.println(state);


//    private static void mix1(InternalState state)
//    {
//        state.a -= state.c;
//        state.a ^= rot(state.c, 4);
//        state.c += state.b;
//    }
//
//
//    private static void mix2(InternalState state)
//    {
////        System.out.printf("b - a -> 0x%08x ^ 0x%08x -> 0x%08x\n", state.b, state.a, state.b - state.a);
//        state.b -= state.a;
//
////        System.out.printf("b ^ rot(a, 6) -> 0x%08x ^ 0x%08x -> 0x%08x\n", state.b, rot(state.a, 6), state.b ^ rot(state.a, 6));
//        state.b = state.b ^ rot(state.a, 6);
//
//
//        state.a += state.c;
//    }
//
//
//    private static void mix3(InternalState state)
//    {
//        state.c -= state.b;
//        state.c ^= rot(state.b, 8);
//        state.b += state.a;
//    }
//
//
//    private static void mix4(InternalState state)
//    {
//        state.a -= state.c;
//        state.a ^= rot(state.c, 16);
//        state.c += state.b;
//    }
//
//
//    private static void mix5(InternalState state)
//    {
//        state.b -= state.a;
//        state.b ^= rot(state.a, 19);
//        state.a += state.c;
//    }
//
//
//    private static void mix6(InternalState state)
//    {
//        state.c -= state.b;
//        state.c ^= rot(state.b, 4);
//        state.b += state.a;
//    }
}
