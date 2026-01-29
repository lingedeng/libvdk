#ifndef LIBVDK_VHD_LOG_H_
#define LIBVDK_VHD_LOG_H_

#include <stdint.h>
#include <vector>
#include "utils.h"

namespace vhdx {
namespace header {
class HeaderSection;
}

class Vhdx;

namespace log {

struct LogSequence;

#pragma pack(push, 1)

struct EntryHeader {
    char signature[4];      // MUST be 0x65676F6C ("loge" as UTF8)
    uint32_t checksum;      // A CRC-32C hash computed over the entire entry specified by the EntryLength field
    uint32_t entry_length;  // Specifies the total length of the entry in bytes. The value MUST be a multiple of 4 KB         
    uint32_t tail;          // The offset, in bytes
                            // from the beginning of the log to the beginning log entry of a sequence ending with this entry.
                            // The value MUST be a multiple of 4 KB
    uint64_t seq_num;
    uint64_t desc_count;    // Specifies the number of descriptors that are contained in this log entry
    //uint32_t reserved;      // this field MUST be set to 0, v1.0没有该字段，v5.0存在该字段
    libvdk::guid::GUID   guid;
    uint64_t flushed_file_offset;   // Stores the VHDX file size in bytes that 
                                    // MUST be at least as large as the size of the VHDX file at the time the log entry was written.
                                    // The value MUST be a multiple of 1 MB.
    uint64_t last_file_offset;      // Stores a file size in bytes that all allocated file structures fit into, 
                                    // at the time the log entry was written.    
};

struct Descriptor {
    char signature[4];
    union {
        uint32_t reserved;
        uint32_t trailing_bytes;
    };
    union {
        uint64_t zero_length;
        uint64_t leading_bytes;
    };

    uint64_t file_offset;       // Specifies the file offset to which the data described by this descriptor MUST be written
                                // The value MUST be a multiple of 4 KB
    uint64_t seq_num;           // MUST match the seq_num field of the log entry's header
};

struct ZeroDescriptor {
    char signature[4];      // MUST be 0x6F72657A ("zero" as ASCII)
    uint32_t reserved;      // this field MUST be set to 0
    uint64_t zero_length;   // Specifies the length of the section to zero
                            // The value MUST be a multiple of 4 KB
    uint64_t file_offset;   // Specifies the file offset to which zeros MUST be written
                            // The value MUST be a multiple of 4 KB
    uint64_t seq_num;       // MUST match the seq_num field of the log entry's header
};

struct DataDescriptor {
    char signature[4];          // MUST be 0x63736564 ("desc" as ASCII)
    uint32_t trailing_bytes;    // Contains the four trailing bytes that were removed from the update when it was converted to a data sector
                                // Bytes 4,092 through 4,096 for DataSector.data                                
    uint64_t leading_bytes;     // Contains the first eight bytes that were removed from the update when it was converted to a data sector.
                                // Bytes 0 through 7 for DataSector.data
    uint64_t file_offset;       // Specifies the file offset to which the data described by this descriptor MUST be written
                                // The value MUST be a multiple of 4 KB
    uint64_t seq_num;           // MUST match the seq_num field of the log entry's header
};

struct DataSector {
    char signature[4];      // MUST be 0x61746164 ("data" as ASCII)
    uint32_t seq_high;      // MUST contain the four most significant bytes of the SequenceNumber field of the associated entry
    char data[4084];        // Contains the raw data associated with the update, bytes 8 through 4,091, inclusive
                            // Bytes [0-7] and [4,092-4,096) are stored in the data descriptor, 
                            // in the LeadingBytes and TrailingBytes fields, respectively
    uint32_t seq_low;       // MUST contain the four least significant bytes of the SequenceNumber field of the associated entry
};

struct LogEntries {
    uint64_t offset;
    uint32_t length;
    uint32_t write;
    uint32_t read;
    EntryHeader *hdr;    
    uint64_t seq;
    uint32_t tail;

    LogEntries() : offset(0UL), length(0UL),
        write(0), read(0), hdr(nullptr),
        seq(0UL), tail(0) {}        
};

#pragma pack(pop)

class LogSection {
public:
    LogSection();
    explicit LogSection(int fd, header::HeaderSection* header);
    explicit LogSection(Vhdx* vhdx);
    ~LogSection() = default;

    void initContent(uint32_t file_payload_in_mb, uint64_t seq_num = 0);
    int  writeContent(int fd);
    int  parseContent();
    void setVhdx(Vhdx* v);

    int  writeLogEntryAndFlush(uint64_t offset, const void* data, uint32_t length);
    void show();
private:
    uint32_t calcDescSectors(uint32_t desc_count);
    uint32_t incLogIndex(uint32_t idx, uint64_t log_length);
    void     resetLog();
    int      searchLog(LogSequence* logs);
    int      flushLog(LogSequence* logs);
    int      validateLogEntry(LogEntries* log, uint64_t seq, bool *seq_valid, EntryHeader* hdr);
    int      peekEntryHeader(const LogEntries& log, EntryHeader* hdr);
    bool     validateEntryHeader(const LogEntries& log, const EntryHeader& hdr);    
    int      readDescriptors(LogEntries* log, const EntryHeader& eheader, std::vector<uint8_t>* desc_buf);
    int      readSectors(LogEntries* log, bool peek, std::vector<uint8_t>* sectors_buf, uint32_t num_sectors, uint32_t *readed_sectors);
    int      writeSectors(LogEntries* log, const std::vector<uint8_t>& sectors_buf, uint32_t num_sectors, uint32_t *written_sectors);
    bool     validateDescriptor(const EntryHeader& eheader, const Descriptor& desc);
    int      flushDesciptor(const Descriptor& desc, std::vector<uint8_t>* sectors_buf);
    int      writeLogEntry(uint64_t offset, const void* data, uint32_t length);

    EntryHeader entry_header_;

    int fd_;
    header::HeaderSection *header_;
    Vhdx *vhdx_;

    // save log info after parse content
    LogEntries log_entry_;
};

} // namespace log
} // namespace vhdx

#endif
