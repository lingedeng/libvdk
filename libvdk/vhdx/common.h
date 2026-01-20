#ifndef LIBVDK_VHDX_COMMON_H_
#define LIBVDK_VHDX_COMMON_H_

#include "utils.h"

namespace vhdx {
namespace header {
const char kFileIdentifierSignature[9] = "vhdxfile";
const char kHeaderSignature[5] = "head";
const char kRegionTableHeaderSignature[5] = "regi";

const char kCreator[12] = "libvdk v0.1";

const uint64_t kHeaderSeqNumForCreate = 0x07;

const static uint32_t kHeaderSectionBaseOffset = (64 * libvdk::kKiB);
const static uint32_t kFileIdentifierInitOffset = (0 * kHeaderSectionBaseOffset);
const static uint32_t kHeader1InitOffset = (1 * kHeaderSectionBaseOffset);
const static uint32_t kHeader2InitOffset = (2 * kHeaderSectionBaseOffset);
const static uint32_t kRegion1InitOffset = (3 * kHeaderSectionBaseOffset);
const static uint32_t kRegion2InitOffset = (4 * kHeaderSectionBaseOffset);

const static uint32_t kHeaderSectionSize = (1 * libvdk::kMiB);

} // namespace header

namespace log {

const char kEntryHeaderSignature[5] = "loge";
const char kZeroDescriptorSignature[5] = "zero";
const char kDataDescriptorSignature[5] = "desc";
const char kDataSectorSignature[5] = "data";

const uint64_t kSeqNumForCreate = 0x0aUL;

const uint32_t kLogSectionInitOffset = (1 * libvdk::kMiB);
const uint32_t kLogSectionInitSize = (1 * libvdk::kMiB);

} // namespace log

namespace metadata {


const char kMetadataTableHeaderSignature[9] = "metadata";

const uint32_t kDefaultBlockSize = (32 * libvdk::kMiB);       // 32M
const uint32_t kDefaultLogicalSectorSize = 0x0200;   // 512 bytes
const uint32_t kDefaultPhysicalSectorSize = (4 * libvdk::kKiB); // 4096 bytes

const uint32_t kMetadataSectionInitOffset = (2 * libvdk::kMiB);
const uint32_t kMetadataValueOffsetFromTableHeader = (64 * libvdk::kKiB);
const uint32_t kMetadataSectionInitSize = (1 * libvdk::kMiB);
    
enum class VirtualDiskType {
    kFixed = 2,
    kDynamic = 3,
    kDifferencing = 4,
};

} // namespace metadata

namespace bat {

const uint32_t kBatInitOffsetInMb = 3;     
const uint32_t kBatInitOffsetInBytes = (kBatInitOffsetInMb * libvdk::kMiB);

enum class PayloadBatEntryStatus {
    kBlockNotPresent = 0,
    kBlockUndefined = 1,
    kBlockZero = 2,
    kBlockUnmapped = 3,
    kBlockFullPresent = 6,
    kBlockPartiallyPresent = 7,
};

const char* const kPayloadNotPresent = "Block not present";
const char* const kPayloadUndefined = "Block undefined";
const char* const kPayloadZero = "Block zero";
const char* const kPayloadUnmapped = "Block unmapped";
const char* const kPayloadFullPresent = "Block full present";
const char* const kPayloadPartiallyPresent = "Block partially present";

enum class BitmapBatEntryStatus {
    kBlockNotPresent = 0,
    kBlockPresent = 6,
};

const char* const kBitmapNotPresent = "Block not present";
const char* const kBitmapPresent = "Block present";

using BatEntry = uint64_t;

//const uint32_t kPayloadOffsetShift = 20;
const uint64_t kPayloadOffsetMask  = 0xFFFFFFFFFFF00000UL;
const uint64_t kSectorsPerBitmap = (8 * libvdk::kMiB);

// offset must multiply of 1Mib
inline BatEntry makePayloadBatEntry(PayloadBatEntryStatus status, uint64_t offset) {
    return offset | static_cast<uint8_t>(status);
}

inline BatEntry makeBitmapBatEntry(BitmapBatEntryStatus status, uint64_t offset) {
    return offset | static_cast<uint8_t>(status);
}

inline void payloadBatStatusOffset(BatEntry be, PayloadBatEntryStatus* status, uint64_t* offset) {
    if (status != nullptr) {
        *status = static_cast<PayloadBatEntryStatus>(be & 0x7);
    }
    if (offset != nullptr) {
        *offset = be & kPayloadOffsetMask;
    }
}

inline void bitmapBatStatusOffset(BatEntry be, BitmapBatEntryStatus* status, uint64_t* offset) {
    if (status != nullptr) {
        *status = static_cast<BitmapBatEntryStatus>(be & 0x7);
    }
    if (offset) {
        *offset = be & kPayloadOffsetMask;
    }
}

} // namespace bat

} // namespace vhdx

#endif