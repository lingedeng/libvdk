#include "log.h"

//#include "elk/base/crc32c.h"

#include <cassert>
#include <cinttypes>
#include <fcntl.h>
#include <vector>

#include "common.h"
#include "header.h"
#include "vhdx.h"

namespace vhdx {
namespace log {

struct LogSequence {
    bool valid;
    uint32_t count;
    LogEntries log;
    EntryHeader hdr;

    LogSequence()
        : valid(false), count(0) {
        memset(&hdr, 0, sizeof(EntryHeader));
    }    
};

const uint32_t kLogEntrySectorSize = 4 * libvdk::kKiB;
const uint32_t kLogMinSize = 1 * libvdk::kMiB;

LogSection::LogSection()
    : fd_(-1),
      header_(nullptr),
      vhdx_(nullptr) {
    memset(&entry_header_, 0, sizeof(entry_header_));    
}

LogSection::LogSection(int fd, header::HeaderSection* header)
    : fd_(fd),
      header_(header),
      vhdx_(nullptr) {
    memset(&entry_header_, 0, sizeof(entry_header_));
}

LogSection::LogSection(Vhdx *v)
    : fd_(v->fd()),
      header_(v->headerSection()),
      vhdx_(v) {
    memset(&entry_header_, 0, sizeof(entry_header_));
}

void LogSection::setVhdx(Vhdx* v) {
    fd_ = v->fd();
    header_ = v->headerSection();
    vhdx_ = v;
}

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

int LogSection::parseContent() {
    int ret = 0;

    LogSequence logs;

    log_entry_.hdr = &entry_header_;
    log_entry_.offset = header_->logOffset();
    log_entry_.length = header_->logLength();
    log_entry_.seq = kSeqNumForCreate;

    if (log_entry_.offset < kLogMinSize ||
        log_entry_.offset % kLogMinSize) {
        CONSLOG("log offset: %" PRIu64 " invalid", log_entry_.offset);
        ret = -EINVAL;
        goto exit;
    }

    if (header_->logVersion() != 0) {
        CONSLOG("log version must be zero");
        ret = -EINVAL;
        goto exit;
    }

    /* If either the log guid, or log length is zero,
     * then a replay log is not present */
    if (header_->logGuid() == libvdk::guid::kNullGuid ||
        log_entry_.length == 0) {
        goto exit;
    }

    if (log_entry_.length % kLogMinSize) {
        CONSLOG("log length: %u invalid", log_entry_.length);
        ret = -EINVAL;
        goto exit;
    }

    /* The log is present, we need to find if and where there is an active
     * sequence of valid entries present in the log.  */
    ret = searchLog(&logs);
    if (ret) {
        goto exit;
    }

    if (logs.valid) {
        int val = fcntl(fd_, F_GETFL);
        if (val == -1) {
            CONSLOG("F_GETFL failed");
            ret = -errno;
            goto exit;
        }
        
        if ((val & O_ACCMODE) == O_RDONLY) {
            CONSLOG("file readonly, but contains a log that needs to be replayed");
            ret = -EPERM;
            goto exit;
        }

        ret = flushLog(&logs);
        if (ret) {
            goto exit;          
        }        
    }

exit:
    return ret;
}

int LogSection::searchLog(LogSequence* logs) {
    int ret = 0;    
    uint32_t tail = 0;
    bool seq_valid = false;
    LogSequence candidate;
    EntryHeader hdr;
    LogEntries current_log;

    current_log = log_entry_;
    current_log.read = 0;
    current_log.write = log_entry_.length; /* assume log is full */

    /* now we will go through the whole log sector by sector, until
     * we find a valid, active log sequence, or reach the end of the
     * log buffer */
    for (;;) {
        uint64_t current_seq = 0;
        LogSequence current;

        tail = current_log.read;

        // find first log entry that active_header.log_guid == EntryHeader.guid
        ret = validateLogEntry(&current_log, current_seq, &seq_valid, &hdr);
        if (ret) {
            CONSLOG("validata log entry failed");
            goto exit;
        }

        if (seq_valid) {
            current.valid = true;
            current.log = current_log;
            current.log.read = tail;
            current.log.write = current_log.read;
            current.count = 1;
            current.hdr = hdr;

            for (;;) {
                // from first found log entry and check sequence number serial
                ret = validateLogEntry(&current_log, current_seq, &seq_valid, &hdr);
                if (ret) {
                    CONSLOG("validata log entry failed");
                    goto exit;
                }
                if (!seq_valid) {
                    CONSLOG("seq invalid");
                    break;
                }

                current.log.write = current_log.read;
                current.count++;

                current_seq = hdr.seq_num;
            }
        }

        if (current.valid) {
            if (!candidate.valid ||
                current.hdr.seq_num > candidate.hdr.seq_num) {
                candidate = current;
            }
        }

        if (current_log.read < tail) {
            break;
        }
    }

    *logs = candidate;

    if (candidate.valid) {
        /* this is the next sequence number, for writes */
        log_entry_.seq = candidate.hdr.seq_num + 1;
    }
exit:
    return ret;
}

int LogSection::flushLog(LogSequence* logs) {
    int ret = 0;
    uint32_t cnt, readed_sectors;
    uint64_t new_file_size;
    std::vector<uint8_t> data_sector_buf;
    int64_t file_length;
    EntryHeader tmp_hdr;
    Descriptor *pdesc;

    cnt = logs->count;
    data_sector_buf.resize(kLogEntrySectorSize, 0);
    
    ret = vhdx_->userVisibleWrite();
    if (ret) {
        CONSLOG("user visible write failed");
        goto exit;
    }

    while (cnt--) {
        ret = peekEntryHeader(logs->log, &tmp_hdr);
        if (ret < 0) {
            CONSLOG("peek entry header failed");
            goto exit;
        }

        ret = libvdk::file::get_file_sizes(fd_, &file_length);
        if (ret) {
            CONSLOG("get file length failed");
            goto exit;
        }

        /* if the log shows a FlushedFileOffset larger than our current file
         * size, then that means the file has been truncated / corrupted, and
         * we must refused to open it / use it */
        if (tmp_hdr.flushed_file_offset > static_cast<uint64_t>(file_length)) {
            CONSLOG("file is too small");
            ret = -EINVAL;
            goto exit;
        }

        std::vector<uint8_t> desc_buf;
        ret = readDescriptors(&logs->log, tmp_hdr, &desc_buf);
        if (ret) {
            CONSLOG("read descriptors failed");
            goto exit;
        }

        pdesc = reinterpret_cast<Descriptor*>(desc_buf.data() + sizeof(EntryHeader));
        for (uint32_t i=0; i<tmp_hdr.desc_count; ++i) {
            /* data sector, so read a sector to flush */
            if (memcmp(pdesc[i].signature, kDataDescriptorSignature, sizeof(pdesc[i].signature)) == 0) {
                ret = readSectors(&logs->log, false, &data_sector_buf, 1, &readed_sectors);
                if (ret) {
                    CONSLOG("read sectors failed");
                    goto exit;
                }

                if (readed_sectors != 1) {
                    ret = -EINVAL;
                    CONSLOG("read sectors failed");
                    goto exit;
                }                
            } else {
                memset(data_sector_buf.data(), 0, kLogEntrySectorSize);
            }

            ret = flushDesciptor(pdesc[i], &data_sector_buf);
            if (ret) {
                CONSLOG("flush data sector failed");
                goto exit;
            }
        }

        if (static_cast<uint64_t>(file_length) < tmp_hdr.last_file_offset) {
            new_file_size = tmp_hdr.last_file_offset;
            if (new_file_size % (1 * libvdk::kMiB)) {
                /* round up to nearest 1MB boundary */
                new_file_size = libvdk::convert::roundUp(new_file_size, 1 * libvdk::kMiB);

                ret = libvdk::file::truncate_file(fd_, new_file_size);
                if (ret) {
                    CONSLOG("truncate file to length: %" PRIu64 " failed", new_file_size);
                    goto exit;
                }
            }
        }
    }

    ret = libvdk::file::flush_file(fd_);
    if (ret) {
        CONSLOG("flush file failed");
        goto exit;
    }

    resetLog();
exit:
    return ret;
}

int LogSection::flushDesciptor(const Descriptor& desc, std::vector<uint8_t>* sectors_buf) {
    int ret = 0;
    uint64_t flush_offset;
    uint32_t count = 1; 
    
    if (memcmp(desc.signature, kDataDescriptorSignature, sizeof(desc.signature)) == 0) {
        if (sectors_buf->size() != kLogEntrySectorSize) {
            CONSLOG("sector buf size: %lu mismatch", sectors_buf->size());
            ret = -EINVAL;
            goto exit;
        }

        uint8_t* p = reinterpret_cast<uint8_t*>(sectors_buf->data());
        DataSector* pds = reinterpret_cast<DataSector*>(p);
        uint64_t data_sector_seq = pds->seq_high;
        data_sector_seq <<= 32;
        data_sector_seq |= (pds->seq_low & 0xFFFFFFFF);

        if (data_sector_seq != desc.seq_num) {
            CONSLOG("desc and data sector seq mismatch");
            ret = -EINVAL;
            goto exit;
        }

        memcpy(p, &desc.leading_bytes, sizeof(desc.leading_bytes));
        p += sizeof(desc.leading_bytes);

        p += sizeof(pds->data);        

        memcpy(p, &desc.trailing_bytes, sizeof(desc.trailing_bytes));

    } else if (memcmp(desc.signature, kZeroDescriptorSignature, sizeof(desc.signature)) == 0) {
        count = desc.zero_length / kLogEntrySectorSize;
    } else {
        CONSLOG("unknown descriptor signature");
        ret = -EINVAL;
        goto exit;
    }

    flush_offset = desc.file_offset;

    for (uint32_t i=0; i<count; ++i) {
        ret = libvdk::file::seek_and_write_file(fd_, flush_offset, sectors_buf->data(), kLogEntrySectorSize, SEEK_SET);
        if (ret) {
            CONSLOG("write desc data at offset: %" PRIu64 " failed", flush_offset);
            goto exit;
        }

        flush_offset += kLogEntrySectorSize;
    }

exit:
    return ret;
}

int LogSection::validateLogEntry(LogEntries* log, uint64_t seq, bool *seq_valid, EntryHeader* hdr) {
    int ret = 0;
    EntryHeader eheader, *p_eheader;
    std::vector<uint8_t> desc_buf, data_sector_buf;
    uint32_t i, desc_sectors, total_sectors, crc;

    *seq_valid = false;

    ret = peekEntryHeader(*log, &eheader);
    if (ret) {
        CONSLOG("peek entry header failed");
        goto inc_and_exit;
    }

    if (!validateEntryHeader(*log, eheader)) {
        CONSLOG("validate entry header failed");
        goto inc_and_exit;
    }

    if (seq > 0) {
        if (eheader.seq_num != (seq + 1)) {
            CONSLOG("sequence num mismatch");
            goto inc_and_exit;
        }
    }

    desc_sectors = calcDescSectors(eheader.desc_count);

    /* Read all log sectors, and calculate log checksum */
    total_sectors = eheader.entry_length / kLogEntrySectorSize;

    /* readDescriptors will increment the read idx */
    // desc_buf includes EntryHeader
    ret = readDescriptors(log, eheader, &desc_buf);
    if (ret) {
        CONSLOG("read descriptors failed");
        goto exit;
    }

    p_eheader = reinterpret_cast<EntryHeader*>(desc_buf.data());
    p_eheader->checksum = 0;
    crc = libvdk::encrypt::crc32c(reinterpret_cast<const char*>(desc_buf.data()), desc_buf.size());

    data_sector_buf.resize(kLogEntrySectorSize, 0);
    if (desc_sectors < total_sectors) {
        for (i=0; i<(total_sectors - desc_sectors); ++i) {
            uint32_t readed_sectors = 0;
            ret = readSectors(log, false, &data_sector_buf, 1, &readed_sectors);
            if (ret || readed_sectors != 1) {
                CONSLOG("read data sector failed");
                goto exit;
            }

            crc = libvdk::encrypt::extend_crc32c(crc, reinterpret_cast<const char*>(data_sector_buf.data()), data_sector_buf.size());
        }
    }

    if (crc != eheader.checksum) {
        CONSLOG("log checksum mismatch[%u|%u]", eheader.checksum, crc);        
        goto exit;
    }

    *seq_valid = true;
    *hdr = eheader;

    goto exit;

inc_and_exit:
    log_entry_.read = incLogIndex(log_entry_.read, log_entry_.length);

exit:
    return ret;
}

int LogSection::peekEntryHeader(const LogEntries& log, EntryHeader* hdr) {
    int ret = 0;
    uint64_t offset;
    uint32_t read;

    assert(hdr != nullptr);

    /* peek is only supported on sector boundaries */
    if (log.read % kLogEntrySectorSize) {
        ret = -EFAULT;
        goto exit;
    }

    read = log.read;

    /* we are guaranteed that a) log sectors are 4096 bytes,
     * and b) the log length is a multiple of 1MB. So, there
     * is always a round number of sectors in the buffer */
    if ((read + sizeof(EntryHeader)) > log.length) {
        read = 0;
    }

    if (read == log.write) {
        ret = -EINVAL;
        goto exit;
    }

    offset = log.offset + read;

    ret = libvdk::file::seek_and_read_file(fd_, offset, hdr, sizeof(EntryHeader), SEEK_SET);
    if (ret) {
        CONSLOG("read log entry header at offset: %" PRIu64 " failed", offset);
        goto exit;
    }

exit:
    return ret;
}

bool LogSection::validateEntryHeader(const LogEntries& log, const EntryHeader& hdr) {
    bool valid = false;

    if (memcmp(&hdr.signature, kEntryHeaderSignature, sizeof(hdr.signature)) != 0) {
        CONSLOG("signature mismatch");
        goto exit;
    }

    /* if the individual entry length is larger than the whole log
     * buffer, that is obviously invalid */
    if (log.length < hdr.entry_length) {
        CONSLOG("entry length too long");
        goto exit;
    }

    /* length of entire entry must be in units of 4KB (log sector size) */
    if (hdr.entry_length % kLogEntrySectorSize) {
        CONSLOG("entry length not aligned to log sector");
        goto exit;
    }

    /* per spec, sequence # must be > 0 */
    if (hdr.seq_num == 0) {
        CONSLOG("sequence number is zero");
        goto exit;
    }

    /* log entries are only valid if they match the file-wide log guid
     * found in the active header */
    if (memcmp(&hdr.guid, &header_->logGuid(), sizeof(hdr.guid)) != 0) {
        CONSLOG("log guid mismatch");
        goto exit;
    }

    if ((hdr.desc_count * sizeof(Descriptor)) > hdr.entry_length) {
        CONSLOG("entry length too small");
        goto exit;
    }

    valid = true;
exit:
    return valid;
}

int LogSection::readDescriptors(LogEntries* log, const EntryHeader& eheader, std::vector<uint8_t>* desc_buf) {
    int ret = 0;
    uint32_t desc_sectors;
    uint32_t readed_sectors;
    Descriptor* desc;

    desc_sectors = calcDescSectors(eheader.desc_count);
    desc_buf->resize(desc_sectors * kLogEntrySectorSize, 0);

    ret = readSectors(log, false, desc_buf, desc_sectors, &readed_sectors);
    if (ret) {
        CONSLOG("read desc sectors failed");
        goto free_and_exit;
    }

    if (desc_sectors != readed_sectors) {
        CONSLOG("not read all sectors[%u|%u]", desc_sectors, readed_sectors);
        ret = -EINVAL;
        goto free_and_exit;
    }

    desc = reinterpret_cast<Descriptor*>(desc_buf->data() + sizeof(EntryHeader));
    for (uint32_t i=0; i<eheader.desc_count; ++i) {
        if (!validateDescriptor(eheader, *desc)) {
            CONSLOG("desc index[%u] is invalid", i);
            ret = -EINVAL;
            goto free_and_exit;
        }
    }
    goto exit;

free_and_exit:
    desc_buf->clear();

exit:
    return ret;
}

bool LogSection::validateDescriptor(const EntryHeader& eheader, const Descriptor& desc) {
    bool valid = false;

    if (desc.seq_num != eheader.seq_num) {
        CONSLOG("desc sequence number mismatch");
        goto exit;
    }
    if (desc.file_offset % kLogEntrySectorSize) {
        CONSLOG("desc file offset: %" PRIu64 " not aligned to log sector", desc.file_offset);
        goto exit;
    }

    if (memcmp(desc.signature, kZeroDescriptorSignature, sizeof(desc.signature)) == 0) {
        if ((desc.zero_length % kLogEntrySectorSize) == 0) {
            valid = true;
        } else {
            CONSLOG("desc zero length: %" PRIu64 " is not aligned", desc.zero_length);
        }
    } else if (memcmp(desc.signature, kDataDescriptorSignature, sizeof(desc.signature)) == 0) {
        valid = true;
    }

exit:
    return valid;
}

int LogSection::readSectors(LogEntries* log, bool peek, std::vector<uint8_t>* sectors_buf, uint32_t num_sectors, uint32_t *readed_sectors) {
    int ret = 0;
    uint32_t read;
    uint64_t offset;

    read = log->read;
    *readed_sectors = 0;

    while (num_sectors) {
        if (read == log->write) {
            CONSLOG("reach end, read[%u]|write[%u]", read, log->write);
            break;
        }

        offset = log->offset + read;

        ret = libvdk::file::seek_and_read_file(fd_, offset, sectors_buf->data(), sectors_buf->size(), SEEK_SET);
        if (ret) {
            CONSLOG("read log sector from offset: %" PRIu64 " failed", offset);
            goto exit;
        }

        read = incLogIndex(read, log->length);

        *readed_sectors += 1;
        num_sectors--;
    }

    if (!peek) {
        log->read = read;
    }

exit:
    return ret;
}

int LogSection::writeSectors(LogEntries* log, const std::vector<uint8_t>& sectors_buf, uint32_t num_sectors, uint32_t *written_sectors) {
    int ret = 0;
    uint32_t write;
    uint64_t offset;
    const uint8_t *p = nullptr;
    
    ret = vhdx_->userVisibleWrite();
    if (ret) {
        CONSLOG("user visible write failed");
        goto exit;
    }

    write = log->write;
    p = sectors_buf.data();
    while (num_sectors) {
        offset = log->offset + write;
        write = incLogIndex(write, log->length);

        if (write == log->read) {
            // full
            break;
        }

        ret = libvdk::file::seek_and_write_file(fd_, offset, p, kLogEntrySectorSize, SEEK_SET);
        if (ret) {
            CONSLOG("write log sector at offset: %" PRIu64 " failed", offset);
            goto exit;
        }

        p += kLogEntrySectorSize;

        log->write = write;
        *written_sectors = *written_sectors + 1;
        num_sectors--;
    }

exit:
    return ret;
}

// FIXME: use class member variable: fd_
int LogSection::writeContent(int fd) {
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

/* Prior to sector data for a log entry, there is the header
 * and the descriptors referenced in the header:
 *
 * [] = 4KB sector
 *
 * [ hdr, desc ][   desc   ][ ... ][ data ][ ... ]
 *
 * The first sector in a log entry has a 64 byte header, and
 * up to 126 32-byte descriptors.  If more descriptors than
 * 126 are required, then subsequent sectors can have up to 128
 * descriptors.  Each sector is 4KB.  Data follows the descriptor
 * sectors.
 *
 * This will return the number of sectors needed to encompass
 * the passed number of descriptors in desc_cnt.
 *
 * This will never return 0, even if desc_cnt is 0.
 */
uint32_t LogSection::calcDescSectors(uint32_t desc_count) {
    uint32_t desc_sectors = 0;

    desc_count += 2; // for header space
    desc_sectors = desc_count / 128;
    if (desc_count % 128) {
        desc_sectors++;
    }

    return desc_sectors;
}

/* Index increment for log, based on sector boundaries */
uint32_t LogSection::incLogIndex(uint32_t idx, uint64_t log_length) {
    idx += kLogEntrySectorSize;
    /* we are guaranteed that a) log sectors are 4096 bytes,
     * and b) the log length is a multiple of 1MB. So, there
     * is always a round number of sectors in the buffer */
    return idx >= log_length ? 0 : idx;
}

void LogSection::resetLog() {
    header_->updateHeader(fd_, nullptr, &libvdk::guid::kNullGuid);
}

int LogSection::writeLogEntryAndFlush(uint64_t offset, const void* data, uint32_t length) {
    int ret = 0;
    LogSequence logs;
    logs.count = 1;
    logs.valid = true;

    assert(offset > (1 * libvdk::kMiB));

    /* Make sure data written (new and/or changed blocks) is stable
     * on disk, before creating log entry */
    ret = libvdk::file::flush_file(fd_);
    if (ret) {
        CONSLOG("flush file failed");
        goto exit;
    }

    ret = writeLogEntry(offset, data, length);
    if (ret) {
        CONSLOG("write log entry failed");
        goto exit;
    }
    logs.log = log_entry_;

    /* Make sure log is stable on disk */
    ret = libvdk::file::flush_file(fd_);
    if (ret) {
        CONSLOG("flush file failed");
        goto exit;
    }
    
    ret = flushLog(&logs);
    if (ret) {
        CONSLOG("flush log failed");
        goto exit;
    }
    log_entry_ = logs.log;

exit:
    return ret;
}

int LogSection::writeLogEntry(uint64_t offset, const void* data, uint32_t length) {
    int ret = 0;
    libvdk::guid::GUID new_log_guid;
    uint32_t i, written_sectors = 0;
    uint32_t sector_offset;
    uint32_t desc_sectors, sectors, total_length;
    uint32_t aligned_length;
    uint32_t leading_length = 0;
    uint32_t trailing_length = 0;
    uint32_t partial_sectors = 0;
    uint32_t bytes_written = 0;
    uint64_t file_offset;
    int64_t file_length;
    EntryHeader eh;
    std::vector<uint8_t> log_buf, merged_buf;
    Descriptor* dd = nullptr;
    DataSector* ds = nullptr;    
    const uint8_t *data_tmp = nullptr, *sector_write = nullptr;

    if (header_->logLength() <= 0) {
        CONSLOG("log length invalid");
        ret = -EINVAL;
        goto exit;
    }

    if (libvdk::guid::kNullGuid == header_->logGuid()) {
        libvdk::guid::generate(&new_log_guid);
        header_->updateHeader(fd_, nullptr, &new_log_guid);
    } else {
        /* currently, we require that the log be flushed after
         * every write. */
        ret = -ENOTSUP;
        goto exit;
    }    

    sector_offset = offset % kLogEntrySectorSize;
    file_offset = libvdk::convert::roundDown(offset, kLogEntrySectorSize);

    aligned_length = length;

    /* add in the unaligned head and tail bytes */
    if (sector_offset) {
        leading_length = (kLogEntrySectorSize - sector_offset);
        leading_length = leading_length > length ? length : leading_length;
        aligned_length -= leading_length;
        partial_sectors++;
    }

    sectors = aligned_length / kLogEntrySectorSize;
    trailing_length = aligned_length - (sectors * kLogEntrySectorSize);
    if (trailing_length) {
        partial_sectors++;
    }

    // count of DataSectors
    sectors += partial_sectors;

    ret = libvdk::file::get_file_sizes(fd_, &file_length);
    if (ret) {
        CONSLOG("get file size failed");
        goto exit;
    }
    
    memcpy(eh.signature, kEntryHeaderSignature, sizeof(eh.signature));
    eh.checksum = 0;
    eh.entry_length = 0;
    eh.tail = log_entry_.tail;
    eh.seq_num = log_entry_.seq;
    eh.desc_count = sectors;
    memcpy(&eh.guid, &new_log_guid, sizeof(eh.guid));
    eh.flushed_file_offset = file_length;
    eh.last_file_offset = file_length;

    desc_sectors = calcDescSectors(sectors);
    total_length = (desc_sectors + sectors) * kLogEntrySectorSize;
    eh.entry_length = total_length;

    log_buf.resize(total_length, 0);    

    dd = reinterpret_cast<Descriptor*>(log_buf.data() + sizeof(EntryHeader));
    ds = reinterpret_cast<DataSector*>(log_buf.data() + (desc_sectors * kLogEntrySectorSize));
    data_tmp = reinterpret_cast<const uint8_t *>(data);

    merged_buf.resize(kLogEntrySectorSize, 0);

    for (i=0; i<sectors; ++i) {
        memcpy(dd->signature, kDataDescriptorSignature, sizeof(dd->signature));
        dd->seq_num = log_entry_.seq;
        dd->file_offset = file_offset;

        if (i == 0 && leading_length) {
            /* partial sector at the front of the buffer */
            ret = libvdk::file::seek_file(fd_, file_offset, SEEK_SET);
            if (ret) {
                goto exit;
            }
            ret = libvdk::file::read_file(fd_, merged_buf.data(), kLogEntrySectorSize);
            if (ret) {
                goto exit;
            }
            memcpy(merged_buf.data() + sector_offset, data_tmp, leading_length);
            bytes_written = leading_length;
            sector_write = merged_buf.data();
        } else if (i == sectors - 1 && trailing_length) {
            /* partial sector at the end of the buffer */
            ret = libvdk::file::seek_file(fd_, file_offset + trailing_length, SEEK_SET);
            if (ret) {
                goto exit;
            }

            ret = libvdk::file::read_file(fd_, merged_buf.data() + trailing_length, kLogEntrySectorSize - trailing_length);
            if (ret) {
                goto exit;
            }
            memcpy(merged_buf.data(), data_tmp, trailing_length);
            bytes_written = trailing_length;
            sector_write = merged_buf.data();
        } else {
            bytes_written = kLogEntrySectorSize;
            sector_write = reinterpret_cast<const uint8_t*>(data_tmp);
        }

        /* populate the raw sector data into the proper structures,
         * as well as update the descriptor, and convert to proper
         * endianness */
        memcpy(&dd->leading_bytes, sector_write, sizeof(dd->leading_bytes));
        //CONSLOG("leading bytes: 0x%lX\n", dd->leading_bytes);
        sector_write += sizeof(dd->leading_bytes);
        memcpy(ds->data, sector_write, sizeof(ds->data));
        sector_write += sizeof(ds->data);
        memcpy(&dd->trailing_bytes, sector_write, sizeof(dd->trailing_bytes));
        //CONSLOG("trailing bytes bytes: 0x%08X\n", dd->trailing_bytes);
        sector_write += sizeof(dd->trailing_bytes);

        memcpy(ds->signature, kDataDescriptorSignature, sizeof(ds->signature));
        ds->seq_high = static_cast<uint32_t>(log_entry_.seq >> 32);
        ds->seq_low = static_cast<uint32_t>(log_entry_.seq & 0xFFFFFFFF);
        
        data_tmp += bytes_written;
        ds += 1;
        dd += 1;
        file_offset += kLogEntrySectorSize; 
    }
            
    eh.checksum = libvdk::encrypt::crc32c(reinterpret_cast<const char*>(log_buf.data()), log_buf.size());
    memcpy(log_buf.data(), &eh, sizeof(EntryHeader));
    
    ret = writeSectors(&log_entry_, log_buf, desc_sectors + sectors, &written_sectors);
    if (ret) {
        CONSLOG("write log sectors failed");
        goto exit;
    }
    if (written_sectors != (desc_sectors + sectors)) {
        CONSLOG("write log sectors failed");
        ret = -EINVAL;
        goto exit;
    }

    log_entry_.seq++;
    // write new tail
    log_entry_.tail = log_entry_.write;

exit:
    return ret;
}

void LogSection::show() {
    int ret = 0;    
    EntryHeader hdr;
    Descriptor *pdesc = nullptr;
    uint32_t data_sector_count = 0;
    LogEntries logs;    

    logs.offset = header_->logOffset();
    logs.length = header_->logLength();
    logs.read = 0;
    logs.write = logs.length;

    for (;;) {
        ret = peekEntryHeader(logs, &hdr);
        if (ret) {
            CONSLOG("peek entry header failed");
            goto out;
        }
        if (hdr.entry_length == 0) {
            break;
        }
        printf("=== Log entry at offset[0x%08X] ===\n", logs.read);
        printf("signature         : %s\n", kEntryHeaderSignature);
        printf("checksum          : 0x%08X\n", hdr.checksum);
        printf("entry length      : %u (0x%08X)\n", hdr.entry_length, hdr.entry_length);
        printf("tail              : %u (0x%08X)\n", hdr.tail, hdr.tail);
        printf("sequence num      : %" PRIu64 "\n", hdr.seq_num);
        printf("descriptor count  : %" PRIu64 "\n", hdr.desc_count);
        printf("log guid          : %s\n", libvdk::guid::toWinString(&hdr.guid).c_str());
        printf("flush file offset : %" PRIu64 " (0x%lX)\n", hdr.flushed_file_offset, hdr.flushed_file_offset);
        printf("last file offset  : %" PRIu64 " (0x%lX)\n", hdr.last_file_offset, hdr.last_file_offset);

        std::vector<uint8_t> desc_buf;
        ret = readDescriptors(&logs, hdr, &desc_buf);
        if (ret) {
            CONSLOG("read descriptor failed");
            goto out;
        }

        pdesc = reinterpret_cast<Descriptor*>(desc_buf.data() + sizeof(EntryHeader));
        for (uint32_t i=0; i<hdr.desc_count; ++i) {
            if (memcmp(pdesc[i].signature, kDataDescriptorSignature, sizeof(pdesc[i].signature)) == 0) {
                printf("\tsignature    : %s\n", kDataDescriptorSignature);
                printf("\ttrail bytes  : 0x%08X\n", pdesc[i].trailing_bytes);
                printf("\tlead  bytes  : 0x%lX\n", pdesc[i].leading_bytes);
                data_sector_count++;
            } else if (memcmp(pdesc[i].signature, kZeroDescriptorSignature, sizeof(pdesc[i].signature)) == 0) {
                printf("\tsignature    : %s\n", kZeroDescriptorSignature);
                printf("\tzero length  : %" PRIu64 " (0x%lX)\n", pdesc[i].zero_length, pdesc[i].zero_length);
            } else {
                CONSLOG("unknown desc signature\n");
                goto out;
            }
            printf("\tfile offset  : %" PRIu64 " (0x%lX)\n", pdesc[i].file_offset, pdesc[i].file_offset);
            printf("\tsequence num : %" PRIu64 "\n", pdesc[i].seq_num);
        }
        printf("\n");
        
        logs.read += (static_cast<uint64_t>(data_sector_count) * kLogEntrySectorSize);
        data_sector_count = 0;
    }

out:
    return;
}

} // namespace log
} // namespace vhdx