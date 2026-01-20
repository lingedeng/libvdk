#include "log.h"

//#include "elk/base/crc32c.h"

#include <vector>

#include "common.h"

namespace vhdx {
namespace log {

void LogSection::initContent(uint32_t file_payload_in_mb, uint64_t seq_num/* = 0*/) {
    uint64_t sn = (seq_num != 0 ? seq_num : kSeqNumForCreate);

    EntryHeader* eh = &entry_header_;
    memcpy(eh->signature, kEntryHeaderSignature, sizeof(eh->signature));
    eh->checksum = 0x0;
    eh->entry_length = (4 * libvdk::kKiB);
    eh->tail = 0x0;

    eh->seq_num = sn;
    eh->desc_count = 0x0;

    //libvdk::guid::GUID guid;
    libvdk::guid::generate(&eh->guid);    
        
    eh->flushed_file_offset = ((vhdx::bat::kBatInitOffsetInMb + static_cast<uint64_t>(file_payload_in_mb)) << libvdk::kMibShift);
    eh->last_file_offset = eh->flushed_file_offset;

    std::vector<char> crc_buf(eh->entry_length, '\0');
    memcpy(crc_buf.data(), eh, sizeof(*eh));

    eh->checksum = libvdk::encrypt::crc32c(reinterpret_cast<const char*>(crc_buf.data()), eh->entry_length);
}

int LogSection::parseContent(int fd) {
    return -1;
}

int  LogSection::writeContent(int fd) {
    int ret = 0;
    
    // file identifier
    ret = libvdk::file::seek_file(fd, kLogSectionInitOffset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to offset: %u failed", kLogSectionInitOffset);
        return ret;
    }
    ret = libvdk::file::write_file(fd, &entry_header_, sizeof(entry_header_));
    if (ret) {
        CONSLOG("write log entry header failed");
        return ret;
    }

    return ret;
}

} // namespace log
} // namespace vhdx