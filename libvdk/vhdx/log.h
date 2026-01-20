#ifndef LIBVDK_VHD_LOG_H_
#define LIBVDK_VHD_LOG_H_

#include <stdint.h>
//#include <uuid/uuid.h>
#include "utils.h"

namespace vhdx {
namespace log {

#pragma pack(push, 1)

struct EntryHeader {
    char signature[4];      // MUST be 0x65676F6C ("loge" as UTF8)
    uint32_t checksum;      // A CRC-32C hash computed over the entire entry specified by the EntryLength field
    uint32_t entry_length;  // Specifies the total length of the entry in bytes. The value MUST be a multiple of 4 KB
    // from the beginning of the log to the beginning log entry of a sequence ending with this entry. 
    // The value MUST be a multiple of 4 KB
    uint32_t tail;          // The offset, in bytes
    uint64_t seq_num;
    uint64_t desc_count;    // Specifies the number of descriptors that are contained in this log entry
    //uint32_t reserved;      // this field MUST be set to 0, v1.0没有该字段，v5.0存在该字段
    libvdk::guid::GUID   guid;
    uint64_t flushed_file_offset;   // Stores the VHDX file size in bytes
    uint64_t last_file_offset;      // Stores a file size in bytes that all allocated file structures fit into    
};

struct ZeroDescriptor {
    char signature[4];      // MUST be 0x6F72657A ("zero" as ASCII)
    uint32_t reserved;      // this field MUST be set to 0
    uint64_t zero_length;   // Specifies the length of the section to zero
    uint64_t file_offset;   // Specifies the file offset to which zeros MUST be written
    uint64_t seq_num;       // MUST match the seq_num field of the log entry's header
};

struct DataDescriptor {
    char signature[4];          // MUST be 0x63736564 ("desc" as ASCII)
    uint32_t trailing_bytes;    // Bytes 0 through 7 for DataSector.data
    uint64_t leading_bytes;     // Bytes 4,092 through 4,096 for DataSector.data
    uint64_t file_offset;       // Specifies the file offset to which the data described by this descriptor MUST be written
    uint64_t seq_num;           // MUST match the seq_num field of the log entry's header
};

struct DataSector {
    char signature[4];          // MUST be 0x61746164 ("data" as ASCII)
    uint32_t seq_high;          // MUST contain the four most significant bytes of the SequenceNumber field of the associated entry
    char data[4084];            // Contains the raw data associated with the update, bytes 8 through 4,091, inclusive
    uint32_t seq_low;           // MUST contain the four least significant bytes of the SequenceNumber field of the associated entry
};

#pragma pack(pop)

class LogSection {
public:
    LogSection() = default;
    ~LogSection() = default;

    void initContent(uint32_t file_payload_in_mb, uint64_t seq_num = 0);
    int  writeContent(int fd);
    int  parseContent(int fd);
private:
    EntryHeader entry_header_;
};

} // namespace log
} // namespace vhdx

#endif
