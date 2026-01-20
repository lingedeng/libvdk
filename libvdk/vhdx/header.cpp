#include "header.h"

#include <inttypes.h>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>

#include "common.h"

namespace vhdx {
namespace header {

// const libvdk::guid::GUID kBatRegionGuid = {
//     {
//         .Data1 = 0x2DC27766,
//         .Data2 = 0xF623,
//         .Data3 = 0x4200,    
//         .Data4 = {0x9D, 0x64, 0x11, 0x5E, 0x9B, 0xFD, 0x4A, 0x08},
//     }
// };

const uint8_t kBatRegionGuid[16] = {
    0x66, 0x77, 0xC2, 0x2D, 
    0x23, 0xF6, 
    0x00, 0x42,
    0x9D, 0x64, 0x11, 0x5E, 0x9B, 0xFD, 0x4A, 0x08
};

const uint8_t kMetadataRegionGuid[16] = {
    0x06, 0xA2, 0x7C, 0x8B,
    0x90, 0x47,
    0x9A, 0x4B,
    0xB8, 0xFE, 0x57, 0x5F, 0x05, 0x0F, 0x88, 0x6E
};

HeaderSection::HeaderSection() 
    : current_header_index_(-1), 
      bat_entry_(nullptr),
      metadata_entry_(nullptr) {
    memset(&file_identifier_, 0, sizeof(file_identifier_));
    memset(headers_, 0, sizeof(headers_[0]) * 2);
    memset(region_tables_, 0, sizeof(region_tables_[0]) * 2);    
}

HeaderSection::~HeaderSection () {

}

// A header is current if it is the only valid header or 
// if it is valid and its SequenceNumber field is greater than the other header's SequenceNumber field.
// int HeaderSection::getCurrentHeaderIndex() {
//     int ret = -1;
//     uint64_t seqs[2] = {0UL, 0UL};
//     for (int i = 0; i<2; ++i) {        
//         if (isValidHeader(i)) {
//             seqs[i] = headers_[i].seq_num;
//         }        
//     }

//     if (seqs[0] != 0 && seqs[1] == 0) {
//         ret = 0;
//     } else if (seqs[0] == 0 && seqs[1] != 0) {
//         ret = 1;
//     } else if (seqs[0] != 0 && seqs[1] != 0) {
//         if (seqs[0] >= seqs[1]) {
//             ret = 0;
//         } else {
//             ret = 1;
//         }
//     }

//     return ret;
// }

/*
 * The VHDX spec calls for header updates to be performed twice, so that both
 * the current and non-current header have valid info
 */
int HeaderSection::updateHeader(int fd, const libvdk::guid::GUID* file_rw_guid/* = nullptr*/) {
    int ret = 0;
    assert(file_rw_guid != nullptr && 
        memcmp(file_rw_guid, &libvdk::guid::kNullGuid, sizeof(libvdk::guid::GUID)) != 0);

    ret = updateInactiveHeader(fd, file_rw_guid);
    if (ret) {
        return ret;
    }

    return updateInactiveHeader(fd, file_rw_guid);
}

int HeaderSection::updateInactiveHeader(int fd, const libvdk::guid::GUID* file_rw_guid) {
    int ret = 0;
    int hdr_index = 0;
    uint64_t header_offset = vhdx::header::kHeader1InitOffset;

    Header* active;
    Header* inactive;

    if (current_header_index_ == 0) {
        hdr_index = 1;
        header_offset = vhdx::header::kHeader2InitOffset;
    }

    active = &headers_[current_header_index_];
    inactive = &headers_[hdr_index];

    inactive->seq_num = active->seq_num + 1;

    /* a new file guid must be generated before any file write, including
     * headers */
    inactive->file_write_guid = *file_rw_guid;

    libvdk::guid::generate(&inactive->data_write_guid);    

    ret = writeHeader(fd, header_offset, inactive);
    if (ret) {
        CONSLOG("write header[%d] failed - %d", hdr_index, ret);
        goto exit;
    }
    // TODO: add file flush, make sure write to file    

    current_header_index_ = hdr_index;
exit:
    return ret;
}

int HeaderSection::updateRegionTable(int current_idx) {
    return -1;
}

bool HeaderSection::isValidFileIdentifier() {
    return (memcmp(file_identifier_.signature, kFileIdentifierSignature, sizeof(kFileIdentifierSignature)-1) == 0);
}

// A header is valid if the Signature and Checksum fields both validate correctly.
bool HeaderSection::isValidHeader(int index) {
    if (memcmp(headers_[index].signature, kHeaderSignature, sizeof(kHeaderSignature)-1) != 0) {
        return false;
    }

    uint32_t chksum = headers_[index].checksum;    
    uint32_t new_chksum = calcHeaderCrc(&headers_[index]);

    return chksum == new_chksum;
}

uint32_t HeaderSection::calcHeaderCrc(const Header* header) {
    std::array<char, kHeaderCrcArrayBufSize> array_buf{'\0'};    
    //memset(array_buf.data(), 0, kHeaderCrcArrayBufSize);
    memcpy(array_buf.data(), header, sizeof(Header));

    Header* h = reinterpret_cast<Header *>(array_buf.data());       
    h->checksum = 0;

    uint32_t chksum = libvdk::encrypt::crc32c(reinterpret_cast<const char*>(h), kHeaderCrcArrayBufSize);

    return chksum;
}

uint32_t HeaderSection::calcRegionTableCrc(const RegionTable* region_table) {    
    std::vector<char> vec_buf(kRegionCrcVecBufSize, '\0');            
    memcpy(vec_buf.data(), region_table, sizeof(RegionTable));    

    
    RegionTable* rt = reinterpret_cast<RegionTable *>(vec_buf.data());    
    rt->header.checksum = 0;

    uint32_t chksum = libvdk::encrypt::crc32c(reinterpret_cast<const char*>(vec_buf.data()), kRegionCrcVecBufSize);

    return chksum;
}

void HeaderSection::initContent(uint32_t total_bat_occupy_mb_count, uint64_t init_seq_num /*=0*/) {
    initFileIdentifier();
    initHeader(init_seq_num);
    initRegionTable(total_bat_occupy_mb_count);
}

int  HeaderSection::parseContent(int fd) {
    int ret = parseFileIdentifier(fd);
    if (ret) {
        return ret;
    }

    ret = parseHeader(fd);
    if (ret) {
        return ret;
    }

    ret = parseRegionTable(fd);
    return ret;
}

void HeaderSection::initFileIdentifier() {
    //FileIdentifier* fi = reinterpret_cast<FileIdentifier *>(buf);
    memcpy(file_identifier_.signature, kFileIdentifierSignature, sizeof(file_identifier_.signature));
    
    libvdk::convert::Utf8ToUnicodeWrapper wstr(kCreator);
    memcpy(file_identifier_.creator, wstr.str(), wstr.len());
}

void HeaderSection::initHeader(uint64_t init_seq_num) {
    uint64_t sn = (init_seq_num == 0 ? kHeaderSeqNumForCreate : init_seq_num);    
    
    Header header;
    Header* h = &header;
    memcpy(h->signature, kHeaderSignature, sizeof(h->signature));    

    libvdk::guid::GUID fwrite_guid, dwrite_guid/*, log_guid*/;
    libvdk::guid::generate(&fwrite_guid);
    libvdk::guid::generate(&dwrite_guid);
    //memcpy(&h->file_write_guid.uuid, &fwrite_guid.uuid, sizeof(fwrite_guid.uuid));
    //memcpy(&h->data_write_guid.uuid, &dwrite_guid.uuid, sizeof(dwrite_guid.uuid));
    h->data_write_guid = dwrite_guid;
    h->file_write_guid = fwrite_guid;
    memset(&h->log_guid, 0, sizeof(libvdk::guid::GUID));

    h->log_version = 0;
    h->version = 1;
    h->log_length = vhdx::log::kLogSectionInitOffset;
    h->log_offset = vhdx::log::kLogSectionInitSize;

    for (int i=0; i<2; ++i) {        
        h->checksum = 0x0;    
        h->seq_num = sn++;        

        memcpy(&headers_[i], h, sizeof(Header));
    }
}

void HeaderSection::initRegionTable(uint32_t total_bat_occupy_mb_count) {
    // init region table & entry buffer          

    RegionTable tmp_rt;    
    RegionTableHeader* r = &tmp_rt.header;
    memcpy(r->signature, kRegionTableHeaderSignature, sizeof(r->signature));
    r->checksum = 0x0;
    r->entry_count = 2;
    r->reserved = 0;
        
    RegionTableEntry* re = &tmp_rt.entries[0];
    // bat region
    memcpy(&re->guid.uuid, kBatRegionGuid, sizeof(kBatRegionGuid));
    re->file_offset = vhdx::bat::kBatInitOffsetInBytes;   
    re->length = (total_bat_occupy_mb_count << libvdk::kMibShift);
    re->required = 1;    
    
    re = &tmp_rt.entries[1];
    // Metadata region
    memcpy(&re->guid.uuid, kMetadataRegionGuid, sizeof(kMetadataRegionGuid));
    re->file_offset = vhdx::metadata::kMetadataSectionInitOffset; // 2M
    re->length = vhdx::metadata::kMetadataSectionInitSize;
    re->required = 1; 

    r->checksum = calcRegionTableCrc(&tmp_rt);

    // copy to second region table
    memcpy(&region_tables_[0], r, sizeof(region_tables_[0]));
    memcpy(&region_tables_[1], r, sizeof(region_tables_[0]));
}

int HeaderSection::parseFileIdentifier(int fd) {
    int ret = libvdk::file::read_file(fd, 
                static_cast<void *>(&file_identifier_), sizeof(file_identifier_));

    if (memcmp(file_identifier_.signature, kFileIdentifierSignature, sizeof(file_identifier_.signature)) != 0) {
        CONSLOG("file identifier signature mismatch");
        ret = -1;        
    }

    return ret;
}

int HeaderSection::parseHeader(int fd) {
    int ret = 0;
    off64_t offset = vhdx::header::kHeader1InitOffset; // 64K    
    //std::array<uint8_t, kHeaderCrcArrayBufSize> array_buf; 
    Header tmp_header;
    uint64_t max_seq_num = 0UL;    

    for (int i=0; i<2; ++i) {
        ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
        if (ret) {
            CONSLOG("seek header offset: 0x%lx failed - %d", offset, ret);
            break;
        }               

        //ret = libvdk::file::read_file(fd, static_cast<void *>(array_buf.data()), kHeaderCrcArrayBufSize);
        ret = libvdk::file::read_file(fd, static_cast<void *>(&tmp_header), sizeof(Header));
        if (ret) {
            CONSLOG("read header[%d] failed - %d", i, ret);
            break;
        }

        //Header* h = reinterpret_cast<Header *>(array_buf.data());
        Header* h = &tmp_header;
        if (memcmp(h->signature, kHeaderSignature, sizeof(h->signature)) != 0) {
            CONSLOG("header[%d] signature mismatch", i);
            ret = -1;
            break;
        }
        
        uint32_t chksum = h->checksum;        
        uint32_t new_chksum = calcHeaderCrc(h);        
        if (chksum != new_chksum) {
            CONSLOG("header[%d] checksum[0x%X|0x%X] mismatch", i, chksum, new_chksum);
            ret = -1;
            break;
        }        
        
        memcpy(&headers_[i], h, sizeof(headers_[0]));

        if (h->seq_num > max_seq_num) {
            current_header_index_ = i;
            max_seq_num = h->seq_num;
        }

        offset += 64 * libvdk::kKiB;
    }

    return ret;
}

int HeaderSection::parseRegionTable(int fd) {
    int ret = 0;
    off64_t offset = vhdx::header::kRegion1InitOffset; // 192K    
    RegionTable tmp_rt;

    for (int i=0; i<2; ++i) {
        ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
        if (ret) {
            CONSLOG("seek region offset: 0x%lx failed - %d", offset, ret);
            break;
        }        

        ret = libvdk::file::read_file(fd, static_cast<void *>(&tmp_rt), sizeof(RegionTable));
        if (ret) {
            CONSLOG("read region[%d] failed - %d", i, ret);
            break;
        }
        
        RegionTable* rt = &tmp_rt;
        if (memcmp(rt->header.signature, kRegionTableHeaderSignature, sizeof(rt->header.signature)) != 0) {
            CONSLOG("region[%d] signature mismatch", i);
            ret = -1;
            break;
        }

        uint32_t chksum = rt->header.checksum;        
        uint32_t new_chksum = calcRegionTableCrc(rt);
        if (chksum != new_chksum) {
            CONSLOG("region[%d] checksum[0x%X|0x%X] mismatch", i, chksum, new_chksum);
            ret = -1;
            break;
        }        

        if (memcmp(&rt->entries[0].guid.uuid, kBatRegionGuid, sizeof(kBatRegionGuid)) != 0 &&
            memcmp(&rt->entries[1].guid.uuid, kBatRegionGuid, sizeof(kBatRegionGuid)) != 0) {

            CONSLOG("region[%d] not content BAT regions", i);
            ret = -1;
            break;
        }

        if (memcmp(&rt->entries[0].guid.uuid, kMetadataRegionGuid, sizeof(kMetadataRegionGuid)) != 0 &&
            memcmp(&rt->entries[1].guid.uuid, kMetadataRegionGuid, sizeof(kMetadataRegionGuid)) != 0) {

            CONSLOG("region[%d] not content Metadata regions", i);
            ret = -1;
            break;
        }

        memcpy(&region_tables_[i], rt, sizeof(region_tables_[0]));

        offset += 64 * libvdk::kKiB;
    }

    if (!ret) {
        RegionTable* rt = &region_tables_[0];
        if (memcmp(&rt->entries[0].guid.uuid, kBatRegionGuid, sizeof(kBatRegionGuid)) == 0) {
            bat_entry_ = &rt->entries[0];
            metadata_entry_ = &rt->entries[1];
        } else {
            bat_entry_ = &rt->entries[1];
            metadata_entry_ = &rt->entries[0];
        }
    }

    return ret;
}

void HeaderSection::show() const {
    showFileIdentifier();
    showHeader();
    showRegion();
}

void HeaderSection::showFileIdentifier() const {
    printf("=== file identifier ===\n");
    printf("signature : %s\n", kFileIdentifierSignature);
    printf("creator   : %s\n\n",
        libvdk::convert::wchar2Utf8(reinterpret_cast<const wchar_t*>(file_identifier_.creator)).c_str());
}

void HeaderSection::showHeader() const {
    for (int i=0; i<2; ++i) {
        const Header* h = &headers_[i];
        printf("=== Header[%d] ===\n", i);
        printf("signature       : %s\n", kHeaderSignature);
        printf("checksum        : 0x%X\n", h->checksum);
        printf("SequenceNumber  : %" PRIu64 "(0x%lX)\n", h->seq_num, h->seq_num);
        printf("file write guid : %s\n", libvdk::guid::toWinString(&h->file_write_guid).c_str());
        printf("data write guid : %s\n", libvdk::guid::toWinString(&h->data_write_guid).c_str());
        printf("log guid        : %s\n", libvdk::guid::toWinString(&h->log_guid).c_str());
        printf("log version     : %d\n", h->log_version);
        printf("file version    : %d\n", h->version);
        printf("log length      : %u(0x%X)\n", h->log_length, h->log_length);
        printf("log offset      : %" PRIu64 "(0x%lX)\n\n", h->log_offset, h->log_offset);
    }
}

void HeaderSection::showRegion() const {
    for (int i=0; i<2; ++i) {
        const RegionTable* rt = &region_tables_[i];
        printf("=== Region header[%d] === \n", i);
        printf("signature   : %s\n", kRegionTableHeaderSignature);
        printf("checksum    : 0x%08X\n", rt->header.checksum);
        printf("entry count : %u\n", rt->header.entry_count);

        for (int j=0; j<2; ++j) {
            printf("Region entry[%d]\n", j);
            printf("\tguid        : %s(%s)\n", libvdk::guid::toWinString(&rt->entries[j].guid).c_str(),
                (memcmp(rt->entries[j].guid.uuid, kBatRegionGuid, sizeof(kBatRegionGuid)) == 0 ? "BAT" : "Metadata"));
            printf("\tfile offset : %" PRIu64 "(0x%lX)\n", rt->entries[j].file_offset, rt->entries[j].file_offset);
            printf("\tlength      : %u(0x%X)\n", rt->entries[j].length, rt->entries[j].length);
            printf("\trequired    : %u\n\n", rt->entries[j].required);
        }        
    }
}

int HeaderSection::writeHeader(int fd, uint64_t offset, Header* h) {
    int ret = 0;

    h->checksum = calcHeaderCrc(h);

    ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to offset: %" PRIu64 " failed", offset);
        return ret;
    }
    ret = libvdk::file::write_file(fd, h, sizeof(*h));
    if (ret) {
        CONSLOG("write header failed");
        return ret;
    }

    return ret;
}

int HeaderSection::writeRegionTable(int fd, uint64_t offset, RegionTable* rt) {
    int ret = 0;

    //rt->header.checksum = calcRegionTableCrc(rt);

    ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to offset: %" PRIu64 " failed", offset);
        return ret;
    }
    ret = libvdk::file::write_file(fd, rt, sizeof(*rt));
    if (ret) {
        CONSLOG("write region table failed");
        return ret;
    }

    return ret;
}

int HeaderSection::writeContent(int fd) {
    int ret = 0;
    
    // file identifier
    ret = libvdk::file::seek_file(fd, kFileIdentifierInitOffset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to offset: %u failed", kFileIdentifierInitOffset);
        return ret;
    }
    ret = libvdk::file::write_file(fd, &file_identifier_, sizeof(file_identifier_));
    if (ret) {
        CONSLOG("write file identifier failed");
        return ret;
    }

    // header
    uint32_t offset;
    for (int i=0; i<2; ++i) {
        offset = (i == 0 ? kHeader1InitOffset : kHeader2InitOffset);         

        ret = writeHeader(fd, offset, &headers_[i]);
        if (ret) {
            return ret;
        }
    }

    for (int i=0; i<2; ++i) {
        offset = (i == 0 ? kRegion1InitOffset : kRegion2InitOffset);

        ret = writeRegionTable(fd, offset, &region_tables_[i]);
        if (ret) {
            return ret;
        }
    }

    return ret;
}

} // namespace header
} // namespace vhdx