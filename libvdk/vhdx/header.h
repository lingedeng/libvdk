#ifndef LIBVDK_VHD_HEADER_H_
#define LIBVDK_VHD_HEADER_H_

#include <cstdint>
#include <memory>
#include "utils.h"

namespace vhdx {
namespace header {

#pragma pack(push, 1)

struct FileIdentifier { // length = 64KB
    char signature[8];  // "vhdxfile"
    char creator[512];
    //char reserved[65536 - 512 - 8];    
};

struct Header {     // length = 64KB
    char        signature[4];   // "head"
    uint32_t    checksum;       // checksum for 4k data
    uint64_t    seq_num;
    libvdk::guid::GUID      file_write_guid;
    libvdk::guid::GUID      data_write_guid;
    libvdk::guid::GUID      log_guid;   // Specifies a 128-bit unique identifier used to determine the validity of log entries
                                        // If this field is zero, then the log is empty or has no valid entries and MUST not be replayed
                                        // Otherwise, only log entries that contain this identifier in their header are valid log entries
    uint16_t    log_version;    
    uint16_t    version;        // must be 1
    uint32_t    log_length;     // Specifies the size, in bytes of the log. This value MUST be a multiple of 1MB
    uint64_t    log_offset;
    // char        reserved4k[4016];   // MUST be set to 0 and ignored
    // char        reserved[65536 - 4096];
};

struct RegionTableHeader { // length = 64KB
    char        signature[4];   // "regi"
    uint32_t    checksum;       // checksum for 64k data
    uint32_t    entry_count;    // This MUST be less than or equal to 2,047
    uint32_t    reserved;       // MUST be set to 0 and ignored
};

/*
Known Regions	    GUID	                           IsRequired
BAT	            2DC27766-F623-4200-9D64-115E9BFD4A08	True
Metadata region	8B7CA206-4790-4B9A-B8FE-575F050F886E	True
*/
struct RegionTableEntry {
    libvdk::guid::GUID      guid;           // Specifies a 128-bit identifier for the object
    uint64_t    file_offset;    // The value MUST be a multiple of 1 MB and MUST be at least 1 MB
    uint32_t    length;         // The value MUST be a multiple of 1 MB
    uint32_t    required;       // Specifies whether this region must be recognized by the implementation in order to load the VHDX file
};

struct RegionTable {
    RegionTableHeader header;
    RegionTableEntry  entries[2];    
    // std::unique_ptr<RegionTableEntry> up_entries;
    // RegionTableEntry  entries[2047];
    // char              reserved[65536 - sizeof(RegionTableEntry)*2 - sizeof(RegionTableHeader)];
};

#pragma pack(pop)

class HeaderSection {
public:
    HeaderSection();
    ~HeaderSection();

    void initContent(uint32_t total_bat_occupy_mb_count, uint64_t init_seq_num = 0);
    int  writeContent(int fd);
    int  parseContent(int fd);    

    const libvdk::guid::GUID& activeHeaderDataWriteGuid() const {
        return headers_[active_header_index_].data_write_guid;
    }
    Header& header(int current_idx) {
        return headers_[current_idx];
    }

    RegionTable& regionTable(int current_idx) {
        return region_tables_[current_idx];
    }

    RegionTableEntry& batEntry() {
        return *bat_entry_;
    }

    RegionTableEntry& metadataEntry() {
        return *metadata_entry_;
    }

    int getCurrentHeaderIndex() {
        return active_header_index_;
    }

    uint32_t logLength() const {
        return headers_[active_header_index_].log_length;
    }

    uint64_t logOffset() const {
        return headers_[active_header_index_].log_offset;
    }

    const libvdk::guid::GUID& logGuid() const {
        return headers_[active_header_index_].log_guid;
    }

    uint32_t logVersion() const {
        return headers_[active_header_index_].log_version;
    }

    int updateHeader(int fd, const libvdk::guid::GUID* file_rw_guid = nullptr, const libvdk::guid::GUID* log_guid = nullptr);
    int updateRegionTable(int current_idx);

    void show() const;    

private:
    const static size_t kHeaderCrcArrayBufSize = (4 * libvdk::kKiB);
    const static size_t kRegionCrcVecBufSize = (64 * libvdk::kKiB);    

    bool isValidFileIdentifier();
    bool isValidHeader(int index);

    void initFileIdentifier();
    void initHeader(uint64_t init_seq_num);
    void initRegionTable(uint32_t total_bat_occupy_mb_count);

    int parseFileIdentifier(int fd);
    int parseHeader(int fd);
    int parseRegionTable(int fd);

    void showFileIdentifier() const;
    void showHeader() const;
    void showRegion() const;

    int  updateInactiveHeader(int fd, const libvdk::guid::GUID* file_rw_guid, const libvdk::guid::GUID* log_guid);
    int  writeHeader(int fd, uint64_t offset, Header* h);
    int  writeRegionTable(int fd, uint64_t offset, RegionTable* rt);
    uint32_t calcHeaderCrc(const Header* header);
    uint32_t calcRegionTableCrc(const RegionTable* header);

    FileIdentifier file_identifier_;
    Header headers_[2];
    RegionTable region_tables_[2];

    int32_t active_header_index_;
    RegionTableEntry* bat_entry_;
    RegionTableEntry* metadata_entry_;    
};

} // namespace head
} // namespace vhdx

#endif