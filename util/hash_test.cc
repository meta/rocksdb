//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/hash.h"

#include <cstring>
#include <type_traits>
#include <vector>

#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/coding_lean.h"
#include "util/hash128.h"
#include "util/math.h"
#include "util/math128.h"

using ROCKSDB_NAMESPACE::BijectiveHash2x64;
using ROCKSDB_NAMESPACE::BijectiveUnhash2x64;
using ROCKSDB_NAMESPACE::DecodeFixed64;
using ROCKSDB_NAMESPACE::EncodeFixed32;
using ROCKSDB_NAMESPACE::EndianSwapValue;
using ROCKSDB_NAMESPACE::GetSliceHash64;
using ROCKSDB_NAMESPACE::Hash;
using ROCKSDB_NAMESPACE::Hash128;
using ROCKSDB_NAMESPACE::Hash2x64;
using ROCKSDB_NAMESPACE::Hash64;
using ROCKSDB_NAMESPACE::Lower32of64;
using ROCKSDB_NAMESPACE::Lower64of128;
using ROCKSDB_NAMESPACE::ReverseBits;
using ROCKSDB_NAMESPACE::Slice;
using ROCKSDB_NAMESPACE::Unsigned128;
using ROCKSDB_NAMESPACE::Upper32of64;
using ROCKSDB_NAMESPACE::Upper64of128;

// The hash algorithm is part of the file format, for example for the Bloom
// filters. Test that the hash values are stable for a set of random strings of
// varying lengths.
TEST(HashTest, Values) {
  constexpr uint32_t kSeed = 0xbc9f1d34;  // Same as BloomHash.

  EXPECT_EQ(Hash("", 0, kSeed), 3164544308u);
  EXPECT_EQ(Hash("\x08", 1, kSeed), 422599524u);
  EXPECT_EQ(Hash("\x17", 1, kSeed), 3168152998u);
  EXPECT_EQ(Hash("\x9a", 1, kSeed), 3195034349u);
  EXPECT_EQ(Hash("\x1c", 1, kSeed), 2651681383u);
  EXPECT_EQ(Hash("\x4d\x76", 2, kSeed), 2447836956u);
  EXPECT_EQ(Hash("\x52\xd5", 2, kSeed), 3854228105u);
  EXPECT_EQ(Hash("\x91\xf7", 2, kSeed), 31066776u);
  EXPECT_EQ(Hash("\xd6\x27", 2, kSeed), 1806091603u);
  EXPECT_EQ(Hash("\x30\x46\x0b", 3, kSeed), 3808221797u);
  EXPECT_EQ(Hash("\x56\xdc\xd6", 3, kSeed), 2157698265u);
  EXPECT_EQ(Hash("\xd4\x52\x33", 3, kSeed), 1721992661u);
  EXPECT_EQ(Hash("\x6a\xb5\xf4", 3, kSeed), 2469105222u);
  EXPECT_EQ(Hash("\x67\x53\x81\x1c", 4, kSeed), 118283265u);
  EXPECT_EQ(Hash("\x69\xb8\xc0\x88", 4, kSeed), 3416318611u);
  EXPECT_EQ(Hash("\x1e\x84\xaf\x2d", 4, kSeed), 3315003572u);
  EXPECT_EQ(Hash("\x46\xdc\x54\xbe", 4, kSeed), 447346355u);
  EXPECT_EQ(Hash("\xd0\x7a\x6e\xea\x56", 5, kSeed), 4255445370u);
  EXPECT_EQ(Hash("\x86\x83\xd5\xa4\xd8", 5, kSeed), 2390603402u);
  EXPECT_EQ(Hash("\xb7\x46\xbb\x77\xce", 5, kSeed), 2048907743u);
  EXPECT_EQ(Hash("\x6c\xa8\xbc\xe5\x99", 5, kSeed), 2177978500u);
  EXPECT_EQ(Hash("\x5c\x5e\xe1\xa0\x73\x81", 6, kSeed), 1036846008u);
  EXPECT_EQ(Hash("\x08\x5d\x73\x1c\xe5\x2e", 6, kSeed), 229980482u);
  EXPECT_EQ(Hash("\x42\xfb\xf2\x52\xb4\x10", 6, kSeed), 3655585422u);
  EXPECT_EQ(Hash("\x73\xe1\xff\x56\x9c\xce", 6, kSeed), 3502708029u);
  EXPECT_EQ(Hash("\x5c\xbe\x97\x75\x54\x9a\x52", 7, kSeed), 815120748u);
  EXPECT_EQ(Hash("\x16\x82\x39\x49\x88\x2b\x36", 7, kSeed), 3056033698u);
  EXPECT_EQ(Hash("\x59\x77\xf0\xa7\x24\xf4\x78", 7, kSeed), 587205227u);
  EXPECT_EQ(Hash("\xd3\xa5\x7c\x0e\xc0\x02\x07", 7, kSeed), 2030937252u);
  EXPECT_EQ(Hash("\x31\x1b\x98\x75\x96\x22\xd3\x9a", 8, kSeed), 469635402u);
  EXPECT_EQ(Hash("\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", 8, kSeed), 3530274698u);
  EXPECT_EQ(Hash("\xbb\x18\x5d\xf4\x12\x03\xf7\x99", 8, kSeed), 1974545809u);
  EXPECT_EQ(Hash("\x80\xd4\x3b\x3b\xae\x22\xa2\x78", 8, kSeed), 3563570120u);
  EXPECT_EQ(Hash("\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", 9, kSeed),
            2706087434u);
  EXPECT_EQ(Hash("\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", 9, kSeed),
            1534654151u);
  EXPECT_EQ(Hash("\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", 9, kSeed),
            2355554696u);
  EXPECT_EQ(Hash("\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", 9, kSeed),
            1400800912u);
  EXPECT_EQ(Hash("\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", 10, kSeed),
            3420325137u);
  EXPECT_EQ(Hash("\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", 10, kSeed),
            3427803584u);
  EXPECT_EQ(Hash("\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", 10, kSeed),
            1152407945u);
  EXPECT_EQ(Hash("\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", 10, kSeed),
            3382479516u);
}

// The hash algorithm is part of the file format, for example for the Bloom
// filters.
TEST(HashTest, Hash64Misc) {
  constexpr uint32_t kSeed = 0;  // Same as GetSliceHash64

  for (char fill : {'\0', 'a', '1', '\xff'}) {
    const size_t max_size = 1000;
    const std::string str(max_size, fill);

    for (size_t size = 0; size <= max_size; ++size) {
      uint64_t here = Hash64(str.data(), size, kSeed);

      // Must be same as unseeded Hash64 and GetSliceHash64
      EXPECT_EQ(here, Hash64(str.data(), size));
      EXPECT_EQ(here, GetSliceHash64(Slice(str.data(), size)));

      // Upper and Lower must reconstruct hash
      EXPECT_EQ(here, (uint64_t{Upper32of64(here)} << 32) | Lower32of64(here));
      EXPECT_EQ(here, (uint64_t{Upper32of64(here)} << 32) + Lower32of64(here));
      EXPECT_EQ(here, (uint64_t{Upper32of64(here)} << 32) ^ Lower32of64(here));

      // Seed changes hash value (with high probability)
      for (uint64_t var_seed = 1; var_seed != 0; var_seed <<= 1) {
        EXPECT_NE(here, Hash64(str.data(), size, var_seed));
      }

      // Size changes hash value (with high probability)
      size_t max_smaller_by = std::min(size_t{30}, size);
      for (size_t smaller_by = 1; smaller_by <= max_smaller_by; ++smaller_by) {
        EXPECT_NE(here, Hash64(str.data(), size - smaller_by, kSeed));
      }
    }
  }
}

// Test that hash values are "non-trivial" for "trivial" inputs
TEST(HashTest, Hash64Trivial) {
  // Thorough test too slow for regression testing
  constexpr bool thorough = false;

  // For various seeds, make sure hash of empty string is not zero.
  constexpr uint64_t max_seed = thorough ? 0x1000000 : 0x10000;
  for (uint64_t seed = 0; seed < max_seed; ++seed) {
    uint64_t here = Hash64("", 0, seed);
    EXPECT_NE(Lower32of64(here), 0u);
    EXPECT_NE(Upper32of64(here), 0u);
  }

  // For standard seed, make sure hash of small strings are not zero
  constexpr uint32_t kSeed = 0;  // Same as GetSliceHash64
  char input[4];
  constexpr int max_len = thorough ? 3 : 2;
  for (int len = 1; len <= max_len; ++len) {
    for (uint32_t i = 0; (i >> (len * 8)) == 0; ++i) {
      EncodeFixed32(input, i);
      uint64_t here = Hash64(input, len, kSeed);
      EXPECT_NE(Lower32of64(here), 0u);
      EXPECT_NE(Upper32of64(here), 0u);
    }
  }
}

// Test that the hash values are stable for a set of random strings of
// varying small lengths.
TEST(HashTest, Hash64SmallValueSchema) {
  constexpr uint32_t kSeed = 0;  // Same as GetSliceHash64

  EXPECT_EQ(Hash64("", 0, kSeed), uint64_t{5999572062939766020u});
  EXPECT_EQ(Hash64("\x08", 1, kSeed), uint64_t{583283813901344696u});
  EXPECT_EQ(Hash64("\x17", 1, kSeed), uint64_t{16175549975585474943u});
  EXPECT_EQ(Hash64("\x9a", 1, kSeed), uint64_t{16322991629225003903u});
  EXPECT_EQ(Hash64("\x1c", 1, kSeed), uint64_t{13269285487706833447u});
  EXPECT_EQ(Hash64("\x4d\x76", 2, kSeed), uint64_t{6859542833406258115u});
  EXPECT_EQ(Hash64("\x52\xd5", 2, kSeed), uint64_t{4919611532550636959u});
  EXPECT_EQ(Hash64("\x91\xf7", 2, kSeed), uint64_t{14199427467559720719u});
  EXPECT_EQ(Hash64("\xd6\x27", 2, kSeed), uint64_t{12292689282614532691u});
  EXPECT_EQ(Hash64("\x30\x46\x0b", 3, kSeed), uint64_t{11404699285340020889u});
  EXPECT_EQ(Hash64("\x56\xdc\xd6", 3, kSeed), uint64_t{12404347133785524237u});
  EXPECT_EQ(Hash64("\xd4\x52\x33", 3, kSeed), uint64_t{15853805298481534034u});
  EXPECT_EQ(Hash64("\x6a\xb5\xf4", 3, kSeed), uint64_t{16863488758399383382u});
  EXPECT_EQ(Hash64("\x67\x53\x81\x1c", 4, kSeed),
            uint64_t{9010661983527562386u});
  EXPECT_EQ(Hash64("\x69\xb8\xc0\x88", 4, kSeed),
            uint64_t{6611781377647041447u});
  EXPECT_EQ(Hash64("\x1e\x84\xaf\x2d", 4, kSeed),
            uint64_t{15290969111616346501u});
  EXPECT_EQ(Hash64("\x46\xdc\x54\xbe", 4, kSeed),
            uint64_t{7063754590279313623u});
  EXPECT_EQ(Hash64("\xd0\x7a\x6e\xea\x56", 5, kSeed),
            uint64_t{6384167718754869899u});
  EXPECT_EQ(Hash64("\x86\x83\xd5\xa4\xd8", 5, kSeed),
            uint64_t{16874407254108011067u});
  EXPECT_EQ(Hash64("\xb7\x46\xbb\x77\xce", 5, kSeed),
            uint64_t{16809880630149135206u});
  EXPECT_EQ(Hash64("\x6c\xa8\xbc\xe5\x99", 5, kSeed),
            uint64_t{1249038833153141148u});
  EXPECT_EQ(Hash64("\x5c\x5e\xe1\xa0\x73\x81", 6, kSeed),
            uint64_t{17358142495308219330u});
  EXPECT_EQ(Hash64("\x08\x5d\x73\x1c\xe5\x2e", 6, kSeed),
            uint64_t{4237646583134806322u});
  EXPECT_EQ(Hash64("\x42\xfb\xf2\x52\xb4\x10", 6, kSeed),
            uint64_t{4373664924115234051u});
  EXPECT_EQ(Hash64("\x73\xe1\xff\x56\x9c\xce", 6, kSeed),
            uint64_t{12012981210634596029u});
  EXPECT_EQ(Hash64("\x5c\xbe\x97\x75\x54\x9a\x52", 7, kSeed),
            uint64_t{5716522398211028826u});
  EXPECT_EQ(Hash64("\x16\x82\x39\x49\x88\x2b\x36", 7, kSeed),
            uint64_t{15604531309862565013u});
  EXPECT_EQ(Hash64("\x59\x77\xf0\xa7\x24\xf4\x78", 7, kSeed),
            uint64_t{8601330687345614172u});
  EXPECT_EQ(Hash64("\xd3\xa5\x7c\x0e\xc0\x02\x07", 7, kSeed),
            uint64_t{8088079329364056942u});
  EXPECT_EQ(Hash64("\x31\x1b\x98\x75\x96\x22\xd3\x9a", 8, kSeed),
            uint64_t{9844314944338447628u});
  EXPECT_EQ(Hash64("\x38\xd6\xf7\x28\x20\xb4\x8a\xe9", 8, kSeed),
            uint64_t{10973293517982163143u});
  EXPECT_EQ(Hash64("\xbb\x18\x5d\xf4\x12\x03\xf7\x99", 8, kSeed),
            uint64_t{9986007080564743219u});
  EXPECT_EQ(Hash64("\x80\xd4\x3b\x3b\xae\x22\xa2\x78", 8, kSeed),
            uint64_t{1729303145008254458u});
  EXPECT_EQ(Hash64("\x1a\xb5\xd0\xfe\xab\xc3\x61\xb2\x99", 9, kSeed),
            uint64_t{13253403748084181481u});
  EXPECT_EQ(Hash64("\x8e\x4a\xc3\x18\x20\x2f\x06\xe6\x3c", 9, kSeed),
            uint64_t{7768754303876232188u});
  EXPECT_EQ(Hash64("\xb6\xc0\xdd\x05\x3f\xc4\x86\x4c\xef", 9, kSeed),
            uint64_t{12439346786701492u});
  EXPECT_EQ(Hash64("\x9a\x5f\x78\x0d\xaf\x50\xe1\x1f\x55", 9, kSeed),
            uint64_t{10841838338450144690u});
  EXPECT_EQ(Hash64("\x22\x6f\x39\x1f\xf8\xdd\x4f\x52\x17\x94", 10, kSeed),
            uint64_t{12883919702069153152u});
  EXPECT_EQ(Hash64("\x32\x89\x2a\x75\x48\x3a\x4a\x02\x69\xdd", 10, kSeed),
            uint64_t{12692903507676842188u});
  EXPECT_EQ(Hash64("\x06\x92\x5c\xf4\x88\x0e\x7e\x68\x38\x3e", 10, kSeed),
            uint64_t{6540985900674032620u});
  EXPECT_EQ(Hash64("\xbd\x2c\x63\x38\xbf\xe9\x78\xb7\xbf\x15", 10, kSeed),
            uint64_t{10551812464348219044u});
}

std::string Hash64TestDescriptor(const char* repeat, size_t limit) {
  const char* mod61_encode =
      "abcdefghijklmnopqrstuvwxyz123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  std::string input;
  while (input.size() < limit) {
    input.append(repeat);
  }
  std::string rv;
  for (size_t i = 0; i < limit; ++i) {
    uint64_t h = GetSliceHash64(Slice(input.data(), i));
    rv.append(1, mod61_encode[static_cast<size_t>(h % 61)]);
  }
  return rv;
}

// XXPH3 changes its algorithm for various sizes up through 250 bytes, so
// we need to check the stability of larger sizes also.
TEST(HashTest, Hash64LargeValueSchema) {
  // Each of these derives a "descriptor" from the hash values for all
  // lengths up to 430.
  // Note that "c" is common for the zero-length string.
  EXPECT_EQ(
      Hash64TestDescriptor("foo", 430),
      "cRhyWsY67B6klRA1udmOuiYuX7IthyGBKqbeosz2hzVglWCmQx8nEdnpkvPfYX56Up2OWOTV"
      "lTzfAoYwvtqKzjD8E9xttR2unelbXbIV67NUe6bOO23BxaSFRcA3njGu5cUWfgwOqNoTsszp"
      "uPvKRP6qaUR5VdoBkJUCFIefd7edlNK5mv6JYWaGdwxehg65hTkTmjZoPKxTZo4PLyzbL9U4"
      "xt12ITSfeP2MfBHuLI2z2pDlBb44UQKVMx27LEoAHsdLp3WfWfgH3sdRBRCHm33UxCM4QmE2"
      "xJ7gqSvNwTeH7v9GlC8zWbGroyD3UVNeShMLx29O7tH1biemLULwAHyIw8zdtLMDpEJ8m2ic"
      "l6Lb4fDuuFNAs1GCVUthjK8CV8SWI8Rsz5THSwn5CGhpqUwSZcFknjwWIl5rNCvDxXJqYr");
  // Note that "1EeRk" is common for "Rocks"
  EXPECT_EQ(
      Hash64TestDescriptor("Rocks", 430),
      "c1EeRkrzgOYWLA8PuhJrwTePJewoB44WdXYDfhbk3ZxTqqg25WlPExDl7IKIQLJvnA6gJxxn"
      "9TCSLkFGfJeXehaSS1GBqWSzfhEH4VXiXIUCuxJXxtKXcSC6FrNIQGTZbYDiUOLD6Y5inzrF"
      "9etwQhXUBanw55xAUdNMFQAm2GjJ6UDWp2mISLiMMkLjANWMKLaZMqaFLX37qB4MRO1ooVRv"
      "zSvaNRSCLxlggQCasQq8icWjzf3HjBlZtU6pd4rkaUxSzHqmo9oM5MghbU5Rtxg8wEfO7lVN"
      "5wdMONYecslQTwjZUpO1K3LDf3K3XK6sUXM6ShQQ3RHmMn2acB4YtTZ3QQcHYJSOHn2DuWpa"
      "Q8RqzX5lab92YmOLaCdOHq1BPsM7SIBzMdLgePNsJ1vvMALxAaoDUHPxoFLO2wx18IXnyX");
  EXPECT_EQ(
      Hash64TestDescriptor("RocksDB", 430),
      "c1EeRkukbkb28wLTahwD2sfUhZzaBEnF8SVrxnPVB6A7b8CaAl3UKsDZISF92GSq2wDCukOq"
      "Jgrsp7A3KZhDiLW8dFXp8UPqPxMCRlMdZeVeJ2dJxrmA6cyt99zkQFj7ELbut6jAeVqARFnw"
      "fnWVXOsaLrq7bDCbMcns2DKvTaaqTCLMYxI7nhtLpFN1jR755FRQFcOzrrDbh7QhypjdvlYw"
      "cdAMSZgp9JMHxbM23wPSuH6BOFgxejz35PScZfhDPvTOxIy1jc3MZsWrMC3P324zNolO7JdW"
      "CX2I5UDKjjaEJfxbgVgJIXxtQGlmj2xkO5sPpjULQV4X2HlY7FQleJ4QRaJIB4buhCA4vUTF"
      "eMFlxCIYUpTCsal2qsmnGOWa8WCcefrohMjDj1fjzSvSaQwlpyR1GZHF2uPOoQagiCpHpm");
}

TEST(HashTest, Hash128Misc) {
  constexpr uint32_t kSeed = 0;  // Same as GetSliceHash128

  for (char fill : {'\0', 'a', '1', '\xff', 'e'}) {
    const size_t max_size = 1000;
    std::string str(max_size, fill);

    if (fill == 'e') {
      // Use different characters to check endianness handling
      for (size_t i = 0; i < str.size(); ++i) {
        str[i] += static_cast<char>(i);
      }
    }

    for (size_t size = 0; size <= max_size; ++size) {
      Unsigned128 here = Hash128(str.data(), size, kSeed);

      // Must be same as unseeded Hash128 and GetSliceHash128
      EXPECT_EQ(here, Hash128(str.data(), size));
      EXPECT_EQ(here, GetSliceHash128(Slice(str.data(), size)));
      {
        uint64_t hi;
        uint64_t lo;
        Hash2x64(str.data(), size, &hi, &lo);
        EXPECT_EQ(Lower64of128(here), lo);
        EXPECT_EQ(Upper64of128(here), hi);
      }
      if (size == 16) {
        const uint64_t in_hi = DecodeFixed64(str.data() + 8);
        const uint64_t in_lo = DecodeFixed64(str.data());
        uint64_t hi;
        uint64_t lo;
        BijectiveHash2x64(in_hi, in_lo, &hi, &lo);
        EXPECT_EQ(Lower64of128(here), lo);
        EXPECT_EQ(Upper64of128(here), hi);
        uint64_t un_hi;
        uint64_t un_lo;
        BijectiveUnhash2x64(hi, lo, &un_hi, &un_lo);
        EXPECT_EQ(in_lo, un_lo);
        EXPECT_EQ(in_hi, un_hi);
      }

      // Upper and Lower must reconstruct hash
      EXPECT_EQ(here,
                (Unsigned128{Upper64of128(here)} << 64) | Lower64of128(here));
      EXPECT_EQ(here,
                (Unsigned128{Upper64of128(here)} << 64) ^ Lower64of128(here));

      // Seed changes hash value (with high probability)
      for (uint64_t var_seed = 1; var_seed != 0; var_seed <<= 1) {
        Unsigned128 seeded = Hash128(str.data(), size, var_seed);
        EXPECT_NE(here, seeded);
        // Must match seeded Hash2x64
        {
          uint64_t hi;
          uint64_t lo;
          Hash2x64(str.data(), size, var_seed, &hi, &lo);
          EXPECT_EQ(Lower64of128(seeded), lo);
          EXPECT_EQ(Upper64of128(seeded), hi);
        }
        if (size == 16) {
          const uint64_t in_hi = DecodeFixed64(str.data() + 8);
          const uint64_t in_lo = DecodeFixed64(str.data());
          uint64_t hi;
          uint64_t lo;
          BijectiveHash2x64(in_hi, in_lo, var_seed, &hi, &lo);
          EXPECT_EQ(Lower64of128(seeded), lo);
          EXPECT_EQ(Upper64of128(seeded), hi);
          uint64_t un_hi;
          uint64_t un_lo;
          BijectiveUnhash2x64(hi, lo, var_seed, &un_hi, &un_lo);
          EXPECT_EQ(in_lo, un_lo);
          EXPECT_EQ(in_hi, un_hi);
        }
      }

      // Size changes hash value (with high probability)
      size_t max_smaller_by = std::min(size_t{30}, size);
      for (size_t smaller_by = 1; smaller_by <= max_smaller_by; ++smaller_by) {
        EXPECT_NE(here, Hash128(str.data(), size - smaller_by, kSeed));
      }
    }
  }
}

// Test that hash values are "non-trivial" for "trivial" inputs
TEST(HashTest, Hash128Trivial) {
  // Thorough test too slow for regression testing
  constexpr bool thorough = false;

  // For various seeds, make sure hash of empty string is not zero.
  constexpr uint64_t max_seed = thorough ? 0x1000000 : 0x10000;
  for (uint64_t seed = 0; seed < max_seed; ++seed) {
    Unsigned128 here = Hash128("", 0, seed);
    EXPECT_NE(Lower64of128(here), 0u);
    EXPECT_NE(Upper64of128(here), 0u);
  }

  // For standard seed, make sure hash of small strings are not zero
  constexpr uint32_t kSeed = 0;  // Same as GetSliceHash128
  char input[4];
  constexpr int max_len = thorough ? 3 : 2;
  for (int len = 1; len <= max_len; ++len) {
    for (uint32_t i = 0; (i >> (len * 8)) == 0; ++i) {
      EncodeFixed32(input, i);
      Unsigned128 here = Hash128(input, len, kSeed);
      EXPECT_NE(Lower64of128(here), 0u);
      EXPECT_NE(Upper64of128(here), 0u);
    }
  }
}

std::string Hash128TestDescriptor(const char* repeat, size_t limit) {
  const char* mod61_encode =
      "abcdefghijklmnopqrstuvwxyz123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

  std::string input;
  while (input.size() < limit) {
    input.append(repeat);
  }
  std::string rv;
  for (size_t i = 0; i < limit; ++i) {
    auto h = GetSliceHash128(Slice(input.data(), i));
    uint64_t h2 = Upper64of128(h) + Lower64of128(h);
    rv.append(1, mod61_encode[static_cast<size_t>(h2 % 61)]);
  }
  return rv;
}

// XXH3 changes its algorithm for various sizes up through 250 bytes, so
// we need to check the stability of larger sizes also.
TEST(HashTest, Hash128ValueSchema) {
  // Each of these derives a "descriptor" from the hash values for all
  // lengths up to 430.
  // Note that "b" is common for the zero-length string.
  EXPECT_EQ(
      Hash128TestDescriptor("foo", 430),
      "bUMA3As8n9I4vNGhThXlEevxZlyMcbb6TYAlIKJ2f5ponsv99q962rYclQ7u3gfnRdCDQ5JI"
      "2LrGUaCycbXrvLFe4SjgRb9RQwCfrnmNQ7VSEwSKMnkGCK3bDbXSrnIh5qLXdtvIZklbJpGH"
      "Dqr93BlqF9ubTnOSYkSdx89XvQqflMIW8bjfQp9BPjQejWOeEQspnN1D3sfgVdFhpaQdHYA5"
      "pI2XcPlCMFPxvrFuRr7joaDvjNe9IUZaunLPMewuXmC3EL95h52Ju3D7y9RNKhgYxMTrA84B"
      "yJrMvyjdm3vlBxet4EN7v2GEyjbGuaZW9UL6lrX6PghJDg7ACfLGdxNbH3qXM4zaiG2RKnL5"
      "S3WXKR78RBB5fRFQ8KDIEQjHFvSNsc3GrAEi6W8P2lv8JMTzjBODO2uN4wadVQFT9wpGfV");
  // Note that "35D2v" is common for "Rocks"
  EXPECT_EQ(
      Hash128TestDescriptor("Rocks", 430),
      "b35D2vzvklFVDqJmyLRXyApwGGO3EAT3swhe8XJAN3mY2UVPglzdmydxcba6JI2tSvwO6zSu"
      "ANpjSM7tc9G5iMhsa7R8GfyCXRO1TnLg7HvdWNdgGGBirxZR68BgT7TQsYJt6zyEyISeXI1n"
      "MXA48Xo7dWfJeYN6Z4KWlqZY7TgFXGbks9AX4ehZNSGtIhdO5i58qlgVX1bEejeOVaCcjC79"
      "67DrMfOKds7rUQzjBa77sMPcoPW1vu6ljGJPZH3XkRyDMZ1twxXKkNxN3tE8nR7JHwyqBAxE"
      "fTcjbOWrLZ1irWxRSombD8sGDEmclgF11IxqEhe3Rt7gyofO3nExGckKkS9KfRqsCHbiUyva"
      "JGkJwUHRXaZnh58b4i1Ei9aQKZjXlvIVDixoZrjcNaH5XJIJlRZce9Z9t82wYapTpckYSg");
  EXPECT_EQ(
      Hash128TestDescriptor("RocksDB", 430),
      "b35D2vFUst3XDZCRlSrhmYYakmqImV97LbBsV6EZlOEQpUPH1d1sD3xMKAPlA5UErHehg5O7"
      "n966fZqhAf3hRc24kGCLfNAWjyUa7vSNOx3IcPoTyVRFZeFlcCtfl7t1QJumHOCpS33EBmBF"
      "hvK13QjBbDWYWeHQhJhgV9Mqbx17TIcvUkEnYZxb8IzWNmjVsJG44Z7v52DjGj1ZzS62S2Vv"
      "qWcDO7apvH5VHg68E9Wl6nXP21vlmUqEH9GeWRehfWVvY7mUpsAg5drHHQyDSdiMceiUuUxJ"
      "XJqHFcDdzbbPk7xDvbLgWCKvH8k3MpQNWOmbSSRDdAP6nGlDjoTToYkcqVREHJzztSWAAq5h"
      "GHSUNJ6OxsMHhf8EhXfHtKyUzRmPtjYyeckQcGmrQfFFLidc6cjMDKCdBG6c6HVBrS7H2R");
}

TEST(FastRange32Test, Values) {
  using ROCKSDB_NAMESPACE::FastRange32;
  // Zero range
  EXPECT_EQ(FastRange32(0, 0), 0U);
  EXPECT_EQ(FastRange32(123, 0), 0U);
  EXPECT_EQ(FastRange32(0xffffffff, 0), 0U);

  // One range
  EXPECT_EQ(FastRange32(0, 1), 0U);
  EXPECT_EQ(FastRange32(123, 1), 0U);
  EXPECT_EQ(FastRange32(0xffffffff, 1), 0U);

  // Two range
  EXPECT_EQ(FastRange32(0, 2), 0U);
  EXPECT_EQ(FastRange32(123, 2), 0U);
  EXPECT_EQ(FastRange32(0x7fffffff, 2), 0U);
  EXPECT_EQ(FastRange32(0x80000000, 2), 1U);
  EXPECT_EQ(FastRange32(0xffffffff, 2), 1U);

  // Seven range
  EXPECT_EQ(FastRange32(0, 7), 0U);
  EXPECT_EQ(FastRange32(123, 7), 0U);
  EXPECT_EQ(FastRange32(613566756, 7), 0U);
  EXPECT_EQ(FastRange32(613566757, 7), 1U);
  EXPECT_EQ(FastRange32(1227133513, 7), 1U);
  EXPECT_EQ(FastRange32(1227133514, 7), 2U);
  // etc.
  EXPECT_EQ(FastRange32(0xffffffff, 7), 6U);

  // Big
  EXPECT_EQ(FastRange32(1, 0x80000000), 0U);
  EXPECT_EQ(FastRange32(2, 0x80000000), 1U);
  EXPECT_EQ(FastRange32(4, 0x7fffffff), 1U);
  EXPECT_EQ(FastRange32(4, 0x80000000), 2U);
  EXPECT_EQ(FastRange32(0xffffffff, 0x7fffffff), 0x7ffffffeU);
  EXPECT_EQ(FastRange32(0xffffffff, 0x80000000), 0x7fffffffU);
}

TEST(FastRange64Test, Values) {
  using ROCKSDB_NAMESPACE::FastRange64;
  // Zero range
  EXPECT_EQ(FastRange64(0, 0), 0U);
  EXPECT_EQ(FastRange64(123, 0), 0U);
  EXPECT_EQ(FastRange64(0xffffFFFF, 0), 0U);
  EXPECT_EQ(FastRange64(0xffffFFFFffffFFFF, 0), 0U);

  // One range
  EXPECT_EQ(FastRange64(0, 1), 0U);
  EXPECT_EQ(FastRange64(123, 1), 0U);
  EXPECT_EQ(FastRange64(0xffffFFFF, 1), 0U);
  EXPECT_EQ(FastRange64(0xffffFFFFffffFFFF, 1), 0U);

  // Two range
  EXPECT_EQ(FastRange64(0, 2), 0U);
  EXPECT_EQ(FastRange64(123, 2), 0U);
  EXPECT_EQ(FastRange64(0xffffFFFF, 2), 0U);
  EXPECT_EQ(FastRange64(0x7fffFFFFffffFFFF, 2), 0U);
  EXPECT_EQ(FastRange64(0x8000000000000000, 2), 1U);
  EXPECT_EQ(FastRange64(0xffffFFFFffffFFFF, 2), 1U);

  // Seven range
  EXPECT_EQ(FastRange64(0, 7), 0U);
  EXPECT_EQ(FastRange64(123, 7), 0U);
  EXPECT_EQ(FastRange64(0xffffFFFF, 7), 0U);
  EXPECT_EQ(FastRange64(2635249153387078802, 7), 0U);
  EXPECT_EQ(FastRange64(2635249153387078803, 7), 1U);
  EXPECT_EQ(FastRange64(5270498306774157604, 7), 1U);
  EXPECT_EQ(FastRange64(5270498306774157605, 7), 2U);
  EXPECT_EQ(FastRange64(0x7fffFFFFffffFFFF, 7), 3U);
  EXPECT_EQ(FastRange64(0x8000000000000000, 7), 3U);
  EXPECT_EQ(FastRange64(0xffffFFFFffffFFFF, 7), 6U);

  // Big but 32-bit range
  EXPECT_EQ(FastRange64(0x100000000, 0x80000000), 0U);
  EXPECT_EQ(FastRange64(0x200000000, 0x80000000), 1U);
  EXPECT_EQ(FastRange64(0x400000000, 0x7fffFFFF), 1U);
  EXPECT_EQ(FastRange64(0x400000000, 0x80000000), 2U);
  EXPECT_EQ(FastRange64(0xffffFFFFffffFFFF, 0x7fffFFFF), 0x7fffFFFEU);
  EXPECT_EQ(FastRange64(0xffffFFFFffffFFFF, 0x80000000), 0x7fffFFFFU);

  // Big, > 32-bit range
#if SIZE_MAX == UINT64_MAX
  EXPECT_EQ(FastRange64(0x7fffFFFFffffFFFF, 0x4200000002), 0x2100000000U);
  EXPECT_EQ(FastRange64(0x8000000000000000, 0x4200000002), 0x2100000001U);

  EXPECT_EQ(FastRange64(0x0000000000000000, 420000000002), 0U);
  EXPECT_EQ(FastRange64(0x7fffFFFFffffFFFF, 420000000002), 210000000000U);
  EXPECT_EQ(FastRange64(0x8000000000000000, 420000000002), 210000000001U);
  EXPECT_EQ(FastRange64(0xffffFFFFffffFFFF, 420000000002), 420000000001U);

  EXPECT_EQ(FastRange64(0xffffFFFFffffFFFF, 0xffffFFFFffffFFFF),
            0xffffFFFFffffFFFEU);
#endif
}

TEST(FastRangeGenericTest, Values) {
  using ROCKSDB_NAMESPACE::FastRangeGeneric;
  // Generic (including big and small)
  // Note that FastRangeGeneric is also tested indirectly above via
  // FastRange32 and FastRange64.
  EXPECT_EQ(
      FastRangeGeneric(uint64_t{0x8000000000000000}, uint64_t{420000000002}),
      uint64_t{210000000001});
  EXPECT_EQ(FastRangeGeneric(uint64_t{0x8000000000000000}, uint16_t{12468}),
            uint16_t{6234});
  EXPECT_EQ(FastRangeGeneric(uint32_t{0x80000000}, uint16_t{12468}),
            uint16_t{6234});
  // Not recommended for typical use because for example this could fail on
  // some platforms and pass on others:
  // EXPECT_EQ(FastRangeGeneric(static_cast<unsigned long>(0x80000000),
  //                           uint16_t{12468}),
  //          uint16_t{6234});
}

// for inspection of disassembly
uint32_t FastRange32(uint32_t hash, uint32_t range) {
  return ROCKSDB_NAMESPACE::FastRange32(hash, range);
}

// for inspection of disassembly
size_t FastRange64(uint64_t hash, size_t range) {
  return ROCKSDB_NAMESPACE::FastRange64(hash, range);
}

// Tests for math.h / math128.h (not worth a separate test binary)
using ROCKSDB_NAMESPACE::BitParity;
using ROCKSDB_NAMESPACE::BitsSetToOne;
using ROCKSDB_NAMESPACE::BitwiseAnd;
using ROCKSDB_NAMESPACE::BottomNBits;
using ROCKSDB_NAMESPACE::ConstexprFloorLog2;
using ROCKSDB_NAMESPACE::CountTrailingZeroBits;
using ROCKSDB_NAMESPACE::DecodeFixed128;
using ROCKSDB_NAMESPACE::DecodeFixedGeneric;
using ROCKSDB_NAMESPACE::DownwardInvolution;
using ROCKSDB_NAMESPACE::EncodeFixed128;
using ROCKSDB_NAMESPACE::EncodeFixedGeneric;
using ROCKSDB_NAMESPACE::FloorLog2;
using ROCKSDB_NAMESPACE::Lower64of128;
using ROCKSDB_NAMESPACE::Multiply64to128;
using ROCKSDB_NAMESPACE::Unsigned128;
using ROCKSDB_NAMESPACE::Upper64of128;

int blah(int x) { return DownwardInvolution(x); }

template <typename T1, typename T2>
static void test_BitwiseAnd(T1 v1, T2 v2) {
  auto a = BitwiseAnd(v1, v2);
  // Essentially repeating the implementation :-/
  if constexpr (sizeof(T1) < sizeof(T2)) {
    static_assert(std::is_same_v<decltype(a), T1>);
    EXPECT_EQ(a, static_cast<T1>(v1 & v2));
  } else {
    static_assert(std::is_same_v<decltype(a), T2>);
    EXPECT_EQ(a, static_cast<T2>(v1 & v2));
  }
}

template <typename T>
static void test_BitOps() {
  // This complex code is to generalize to 128-bit values. Otherwise
  // we could just use = static_cast<T>(0x5555555555555555ULL);
  T everyOtherBit = 0;
  for (unsigned i = 0; i < sizeof(T); ++i) {
    everyOtherBit = (everyOtherBit << 8) | T{0x55};
  }

  // This one built using bit operations, as our 128-bit layer
  // might not implement arithmetic such as subtraction.
  T vm1 = 0;  // "v minus one"

  for (int i = 0; i < int{8 * sizeof(T)}; ++i) {
    T v = T{1} << i;
    // If we could directly use arithmetic:
    // T vm1 = static_cast<T>(v - 1);

    // BottomNBits
    {
      // build the mask the extremely slow way
      T bottom_n_mask = 0x00;
      for (int j = 0; j < i; j++) {
        bottom_n_mask <<= 1;
        bottom_n_mask |= 0x1;
      }

      // An essentially full length value
      T x = everyOtherBit;
      if (i > 2) {
        // Make it slightly irregular
        x = x ^ (T{1} << (i / 2));
      }
      auto a = BottomNBits(x, i);
      auto b = BottomNBits(~x, i);

      // check that a and b match the expected values
      EXPECT_EQ(a, x & bottom_n_mask);
      EXPECT_EQ(b, (~x) & bottom_n_mask);

      EXPECT_EQ(x | a, x);
      EXPECT_EQ(a | b, vm1);
      EXPECT_EQ(a & b, T{0});
      EXPECT_EQ(BottomNBits(x ^ a, i), T{0});
    }

    // FloorLog2
    if (v > 0) {
      EXPECT_EQ(FloorLog2(v), i);
      EXPECT_EQ(ConstexprFloorLog2(v), i);
    }
    if (vm1 > 0) {
      EXPECT_EQ(FloorLog2(vm1), i - 1);
      EXPECT_EQ(ConstexprFloorLog2(vm1), i - 1);
      EXPECT_EQ(FloorLog2(everyOtherBit & vm1), (i - 1) & ~1);
      EXPECT_EQ(ConstexprFloorLog2(everyOtherBit & vm1), (i - 1) & ~1);
    }

    // CountTrailingZeroBits
    if (v != 0) {
      EXPECT_EQ(CountTrailingZeroBits(v), i);
    }
    if (vm1 != 0) {
      EXPECT_EQ(CountTrailingZeroBits(vm1), 0);
    }
    if (i < int{8 * sizeof(T)} - 1) {
      EXPECT_EQ(CountTrailingZeroBits(~vm1 & everyOtherBit), (i + 1) & ~1);
    }

    // BitsSetToOne
    EXPECT_EQ(BitsSetToOne(v), 1);
    EXPECT_EQ(BitsSetToOne(vm1), i);
    EXPECT_EQ(BitsSetToOne(vm1 & everyOtherBit), (i + 1) / 2);

    // BitParity
    EXPECT_EQ(BitParity(v), 1);
    EXPECT_EQ(BitParity(vm1), i & 1);
    EXPECT_EQ(BitParity(vm1 & everyOtherBit), ((i + 1) / 2) & 1);

    // EndianSwapValue
    T ev = T{1} << (((sizeof(T) - 1 - (i / 8)) * 8) + i % 8);
    EXPECT_EQ(EndianSwapValue(v), ev);

    // ReverseBits
    EXPECT_EQ(ReverseBits(v), static_cast<T>(T{1} << (8 * sizeof(T) - 1 - i)));
#ifdef HAVE_UINT128_EXTENSION          // Uses multiplication
    if (std::is_unsigned<T>::value) {  // Technical UB on signed type
      T rv = T{1} << (8 * sizeof(T) - 1 - i);
      EXPECT_EQ(ReverseBits(vm1), static_cast<T>(rv * ~T{1}));
    }
#endif

    // DownwardInvolution
    {
      T misc = static_cast<T>(/*random*/ 0xc682cd153d0e3279U +
                              i * /*random*/ 0x9b3972f3bea0baa3U);
      if constexpr (sizeof(T) > 8) {
        misc = (misc << 64) | (/*random*/ 0x52af031a38ced62dU +
                               i * /*random*/ 0x936f803d9752ddc3U);
      }
      T misc_masked = misc & vm1;
      EXPECT_LE(misc_masked, vm1);
      T di_misc_masked = DownwardInvolution(misc_masked);
      EXPECT_LE(di_misc_masked, vm1);
      if (misc_masked > 0) {
        // Highest-order 1 in same position
        EXPECT_EQ(FloorLog2(misc_masked), FloorLog2(di_misc_masked));
      }
      // Validate involution property on short value
      EXPECT_EQ(DownwardInvolution(di_misc_masked), misc_masked);

      // Validate involution property on large value
      T di_misc = DownwardInvolution(misc);
      EXPECT_EQ(DownwardInvolution(di_misc), misc);
      // Highest-order 1 in same position
      if (misc > 0) {
        EXPECT_EQ(FloorLog2(misc), FloorLog2(di_misc));
      }

      // Validate distributes over xor.
      // static_casts to avoid numerical promotion effects.
      EXPECT_EQ(DownwardInvolution(static_cast<T>(misc_masked ^ vm1)),
                static_cast<T>(di_misc_masked ^ DownwardInvolution(vm1)));
      T misc2 = static_cast<T>(misc >> 1);
      EXPECT_EQ(DownwardInvolution(static_cast<T>(misc ^ misc2)),
                static_cast<T>(di_misc ^ DownwardInvolution(misc2)));

      // Choose some small number of bits to pull off to test combined
      // uniqueness guarantee
      int in_bits = i % 7;
      unsigned in_mask = (unsigned{1} << in_bits) - 1U;
      // IMPLICIT: int out_bits = 8 - in_bits;
      std::vector<bool> seen(256, false);
      for (int j = 0; j < 255; ++j) {
        T t_in = misc ^ static_cast<T>(j);
        unsigned in = static_cast<unsigned>(t_in);
        unsigned out = static_cast<unsigned>(DownwardInvolution(t_in));
        unsigned val = ((out << in_bits) | (in & in_mask)) & 255U;
        EXPECT_FALSE(seen[val]);
        seen[val] = true;
      }

      if (i + 8 < int{8 * sizeof(T)}) {
        // Also test manipulating bits in the middle of input is
        // bijective in bottom of output
        seen = std::vector<bool>(256, false);
        for (int j = 0; j < 255; ++j) {
          T in = misc ^ (static_cast<T>(j) << i);
          unsigned val = static_cast<unsigned>(DownwardInvolution(in)) & 255U;
          EXPECT_FALSE(seen[val]);
          seen[val] = true;
        }
      }
    }

    // BitwiseAnd
    {
      test_BitwiseAnd(vm1, static_cast<char>(0x99));
      test_BitwiseAnd(v, static_cast<char>(0x99));
      test_BitwiseAnd(char{0x66}, vm1);
      test_BitwiseAnd(char{0x66}, v);
      test_BitwiseAnd(v, int16_t{0x6699});
      test_BitwiseAnd(v, uint16_t{0x9966});
      test_BitwiseAnd(int64_t{0x1234234534564567}, v);
      test_BitwiseAnd(uint64_t{0x9876876576545432}, v);
    }

    vm1 = (vm1 << 1) | 1;
  }

  // ConstexprFloorLog2
  EXPECT_EQ(ConstexprFloorLog2(T{1}), 0);
  EXPECT_EQ(ConstexprFloorLog2(T{2}), 1);
  EXPECT_EQ(ConstexprFloorLog2(T{3}), 1);
  EXPECT_EQ(ConstexprFloorLog2(T{42}), 5);
}

TEST(MathTest, BitOps) {
  test_BitOps<uint32_t>();
  test_BitOps<uint64_t>();
  test_BitOps<uint16_t>();
  test_BitOps<uint8_t>();
  test_BitOps<unsigned char>();
  test_BitOps<unsigned short>();
  test_BitOps<unsigned int>();
  test_BitOps<unsigned long>();
  test_BitOps<unsigned long long>();
  test_BitOps<char>();
  test_BitOps<size_t>();
  test_BitOps<int32_t>();
  test_BitOps<int64_t>();
  test_BitOps<int16_t>();
  test_BitOps<int8_t>();
  test_BitOps<signed char>();
  test_BitOps<short>();
  test_BitOps<int>();
  test_BitOps<long>();
  test_BitOps<long long>();
  test_BitOps<ptrdiff_t>();
}

TEST(MathTest, BitOps128) { test_BitOps<Unsigned128>(); }

TEST(MathTest, Math128) {
  const Unsigned128 sixteenHexOnes = 0x1111111111111111U;
  const Unsigned128 thirtyHexOnes = (sixteenHexOnes << 56) | sixteenHexOnes;
  const Unsigned128 sixteenHexTwos = 0x2222222222222222U;
  const Unsigned128 thirtyHexTwos = (sixteenHexTwos << 56) | sixteenHexTwos;

  // v will slide from all hex ones to all hex twos
  Unsigned128 v = thirtyHexOnes;
  for (int i = 0; i <= 30; ++i) {
    // Test bitwise operations
    EXPECT_EQ(BitsSetToOne(v), 30);
    EXPECT_EQ(BitsSetToOne(~v), 128 - 30);
    EXPECT_EQ(BitsSetToOne(v & thirtyHexOnes), 30 - i);
    EXPECT_EQ(BitsSetToOne(v | thirtyHexOnes), 30 + i);
    EXPECT_EQ(BitsSetToOne(v ^ thirtyHexOnes), 2 * i);
    EXPECT_EQ(BitsSetToOne(v & thirtyHexTwos), i);
    EXPECT_EQ(BitsSetToOne(v | thirtyHexTwos), 60 - i);
    EXPECT_EQ(BitsSetToOne(v ^ thirtyHexTwos), 60 - 2 * i);

    // Test comparisons
    EXPECT_EQ(v == thirtyHexOnes, i == 0);
    EXPECT_EQ(v == thirtyHexTwos, i == 30);
    EXPECT_EQ(v > thirtyHexOnes, i > 0);
    EXPECT_EQ(v > thirtyHexTwos, false);
    EXPECT_EQ(v >= thirtyHexOnes, true);
    EXPECT_EQ(v >= thirtyHexTwos, i == 30);
    EXPECT_EQ(v < thirtyHexOnes, false);
    EXPECT_EQ(v < thirtyHexTwos, i < 30);
    EXPECT_EQ(v <= thirtyHexOnes, i == 0);
    EXPECT_EQ(v <= thirtyHexTwos, true);

    // Update v, clearing upper-most byte
    v = ((v << 12) >> 8) | 0x2;
  }

  for (int i = 0; i < 128; ++i) {
    // Test shifts
    Unsigned128 sl = thirtyHexOnes << i;
    Unsigned128 sr = thirtyHexOnes >> i;
    EXPECT_EQ(BitsSetToOne(sl), std::min(30, 32 - i / 4));
    EXPECT_EQ(BitsSetToOne(sr), std::max(0, 30 - (i + 3) / 4));
    EXPECT_EQ(BitsSetToOne(sl & sr), i % 2 ? 0 : std::max(0, 30 - i / 2));
  }

  // Test 64x64->128 multiply
  Unsigned128 product =
      Multiply64to128(0x1111111111111111U, 0x2222222222222222U);
  EXPECT_EQ(Lower64of128(product), 2295594818061633090U);
  EXPECT_EQ(Upper64of128(product), 163971058432973792U);
}

TEST(MathTest, Coding128) {
  const char* in = "_1234567890123456";
  // Note: in + 1 is likely unaligned
  Unsigned128 decoded = DecodeFixed128(in + 1);
  EXPECT_EQ(Lower64of128(decoded), 0x3837363534333231U);
  EXPECT_EQ(Upper64of128(decoded), 0x3635343332313039U);
  char out[18];
  out[0] = '_';
  EncodeFixed128(out + 1, decoded);
  out[17] = '\0';
  EXPECT_EQ(std::string(in), std::string(out));
}

TEST(MathTest, CodingGeneric) {
  const char* in = "_1234567890123456";
  // Decode
  // Note: in + 1 is likely unaligned
  Unsigned128 decoded128 = DecodeFixedGeneric<Unsigned128>(in + 1);
  EXPECT_EQ(Lower64of128(decoded128), 0x3837363534333231U);
  EXPECT_EQ(Upper64of128(decoded128), 0x3635343332313039U);

  uint64_t decoded64 = DecodeFixedGeneric<uint64_t>(in + 1);
  EXPECT_EQ(decoded64, 0x3837363534333231U);

  uint32_t decoded32 = DecodeFixedGeneric<uint32_t>(in + 1);
  EXPECT_EQ(decoded32, 0x34333231U);

  uint16_t decoded16 = DecodeFixedGeneric<uint16_t>(in + 1);
  EXPECT_EQ(decoded16, 0x3231U);

  // Encode
  char out[18];
  out[0] = '_';
  memset(out + 1, '\0', 17);
  EncodeFixedGeneric(out + 1, decoded128);
  EXPECT_EQ(std::string(in), std::string(out));

  memset(out + 1, '\0', 9);
  EncodeFixedGeneric(out + 1, decoded64);
  EXPECT_EQ(std::string("_12345678"), std::string(out));

  memset(out + 1, '\0', 5);
  EncodeFixedGeneric(out + 1, decoded32);
  EXPECT_EQ(std::string("_1234"), std::string(out));

  memset(out + 1, '\0', 3);
  EncodeFixedGeneric(out + 1, decoded16);
  EXPECT_EQ(std::string("_12"), std::string(out));
}

int main(int argc, char** argv) {
  fprintf(stderr, "NPHash64 id: %x\n",
          static_cast<int>(ROCKSDB_NAMESPACE::GetSliceNPHash64("RocksDB")));
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
