#include "metadata.h"

#include <sys/stat.h>

#include <cinttypes>
#include <cassert>
#include <cmath>
#include <vector>

#include "common.h"

namespace vhdx {
namespace metadata {

const uint8_t kFileParametersGuid[16] = {
    0x37, 0x67, 0xA1, 0xCA,
    0x36, 0xFA, 
    0x43, 0x4D,
    0xB3, 0xB6, 0x33, 0xF0, 0xAA, 0x44, 0xE7, 0x6B
};

const uint8_t kVirtualDiskSizeGuid[16] = {
    0x24, 0x42, 0xA5, 0x2F,
    0x1B, 0xCD,
    0x76, 0x48,
    0xB2, 0x11, 0x5D, 0xBE, 0xD8, 0x3B, 0xF4, 0xB8
};

const uint8_t kVirtualDiskGuid[16] = {
    0xAB, 0x12, 0xCA, 0xBE,
    0xE6, 0xB2, 
    0x23, 0x45,
    0x93, 0xEF, 0xC3, 0x09, 0xE0, 0x00, 0xC7, 0x46
};

const uint8_t kLogicalSectorSizeGuid[16] = {
    0x1D, 0xBF, 0x41, 0x81,
    0x6F, 0xA9,
    0x09, 0x47,
    0xBA, 0x47, 0xF2, 0x33, 0xA8, 0xFA, 0xAB, 0x5F
};

const uint8_t kPhysicalSectorSizeGuid[16] = {
    0xC7, 0x48, 0xA3, 0xCD,
    0x5D, 0x44,
    0x71, 0x44,
    0x9C, 0xC9, 0xE9, 0x88, 0x52, 0x51, 0xC5, 0x56
};

const uint8_t kParentLocatorGuid[16] = {
    0x2D, 0x5F, 0xD3, 0xA8,
    0x0B, 0xB3,
    0x4D, 0x45,
    0xAB, 0xF7, 0xD3, 0xD8, 0x48, 0x34, 0xAB, 0x0C
};

const wchar_t kParentLocatorLinkage[15] = L"parent_linkage";
const wchar_t kParentLocatorLinkage2[16] = L"parent_linkage2";
const wchar_t kParentLocatorRelativePath[14] = L"relative_path";
const wchar_t kParentLocatorVolumnPath[12] = L"volume_path";
const wchar_t kParentLocatorAbsoluteWin32Path[20] = L"absolute_win32_path";

const uint8_t kLocatorTypeGuid[16] = {
    0xB7, 0xEF, 0x4A, 0xB0,
    0x9E, 0xD1,
    0x81, 0x4A,
    0xB7, 0x89, 0x25, 0xB8, 0xE9, 0x44, 0x59, 0x13
};

const wchar_t* kParentLocatorKeys[5] = {
    kParentLocatorLinkage,
    kParentLocatorAbsoluteWin32Path,
    kParentLocatorRelativePath,
    kParentLocatorLinkage2,
    kParentLocatorVolumnPath
};

MetadataSection::MetadataSection() 
    : chunk_ratio_(0), 
      data_block_count_(0),
      bitmap_block_count_(0),
      total_bat_count_(0UL) {
    memset(&table_header_entries_, 0, sizeof(table_header_entries_));
    memset(&parent_locator_with_data_.locator, 0, sizeof(parent_locator_with_data_.locator));    
}

void MetadataSection::initContent(vhdx::metadata::VirtualDiskType type, 
        uint64_t vdk_size_in_bytes, uint32_t block_size, 
        uint32_t logical_sector_size, uint32_t physical_sector_size) {
    TableHeader* th = &table_header_entries_.table_header_;
    memcpy(th->signature, kMetadataTableHeaderSignature, sizeof(th->signature));
    th->entry_count = 5;
    
    uint32_t te_offset = kMetadataValueOffsetFromTableHeader; // 64K offset
    // File parameters
    TableEntry* te = &table_header_entries_.well_known_table_entries_[0];
    memcpy(&te->item_id, kFileParametersGuid, sizeof(te->item_id));
    te->offset = te_offset; 
    te->length = sizeof(FileParameters);
    te->flags = TableEntryFlags::kIsRequired;

    if (block_size == 0) {
        /* These are pretty arbitrary, and mainly designed to keep the BAT
        * size reasonable to load into RAM */    
        if (vdk_size_in_bytes > 32 * libvdk::kTiB) {
            block_size = 64 * libvdk::kMiB;
        } else if (vdk_size_in_bytes > 100 * libvdk::kGiB) {
            block_size = 32 * libvdk::kMiB;
        } else if (vdk_size_in_bytes > 1 * libvdk::kGiB) {
            block_size = 16 * libvdk::kMiB;
        } else {
            block_size = 8 * libvdk::kMiB;
        }
    }
    file_parameters_.block_size_in_bytes = block_size;    

    switch (type) {
    case vhdx::metadata::VirtualDiskType::kFixed:
        file_parameters_.flags = FileParametersFlags::kLeaveBlockAllocated;
        break;
    case vhdx::metadata::VirtualDiskType::kDifferencing:
        file_parameters_.flags = FileParametersFlags::kHasParent;
        break;
    case vhdx::metadata::VirtualDiskType::kDynamic:
        file_parameters_.flags = static_cast<FileParametersFlags>(0);
        break;
    }    
    te_offset += te->length;

    // Virtual disk size
    te = &table_header_entries_.well_known_table_entries_[1];
    memcpy(&te->item_id, kVirtualDiskSizeGuid, sizeof(te->item_id));
    te->offset = te_offset;
    te->length = sizeof(VirtualDiskSize);
    te->flags = static_cast<TableEntryFlags>(
                    static_cast<uint32_t>(TableEntryFlags::kIsVirtualDisk) | 
                    static_cast<uint32_t>(TableEntryFlags::kIsRequired));

    virtual_disk_size_.size_in_bytes = vdk_size_in_bytes;
    te_offset += te->length;

    // virtual disk guid
    te = &table_header_entries_.well_known_table_entries_[2];
    memcpy(&te->item_id, kVirtualDiskGuid, sizeof(te->item_id));
    te->offset = te_offset;
    te->length = sizeof(VirtualDiskId);
    te->flags = static_cast<TableEntryFlags>(
                    static_cast<uint32_t>(TableEntryFlags::kIsVirtualDisk) | 
                    static_cast<uint32_t>(TableEntryFlags::kIsRequired));
    
    libvdk::guid::generate(&virtual_size_guid_);
    te_offset += te->length;

    // logical sector size
    te = &table_header_entries_.well_known_table_entries_[3];
    memcpy(&te->item_id, kLogicalSectorSizeGuid, sizeof(te->item_id));
    te->offset = te_offset;
    te->length = sizeof(LogicalSectorSize);
    te->flags = static_cast<TableEntryFlags>(
                    static_cast<uint32_t>(TableEntryFlags::kIsVirtualDisk) | 
                    static_cast<uint32_t>(TableEntryFlags::kIsRequired));

    if (logical_sector_size == 0) {
        logical_sector_size_.size_in_bytes = kDefaultLogicalSectorSize;
    } else {
        logical_sector_size_.size_in_bytes = logical_sector_size;
    }
    te_offset += te->length;

    // physical sector size
    te = &table_header_entries_.well_known_table_entries_[4];
    memcpy(&te->item_id, kPhysicalSectorSizeGuid, sizeof(te->item_id));
    te->offset = te_offset;
    te->length = sizeof(PhysicalSectorSize);
    te->flags = static_cast<TableEntryFlags>(
                    static_cast<uint32_t>(TableEntryFlags::kIsVirtualDisk) | 
                    static_cast<uint32_t>(TableEntryFlags::kIsRequired));
    
    if (physical_sector_size == 0) {
        physical_sector_size_.size_in_bytes = kDefaultPhysicalSectorSize;
    } else {
        physical_sector_size_.size_in_bytes = physical_sector_size;
    }
    te_offset += te->length;

    if (type == vhdx::metadata::VirtualDiskType::kDifferencing) {
        initParentLocatorData(th->entry_count);

        th->entry_count += 1;
    }

    calcBatInfo();
}

int MetadataSection::initParentLocatorContent(const std::string& file, const std::string& parent_file, 
    const std::string& linkage, const std::string& parent_absolute_path, const std::string& parent_relative_path) {
    int ret = 0, err;
    struct stat stats;
    char *absolute_path;
    std::string relative_path;       
    
    if (parent_absolute_path.empty()) {
        absolute_path = realpath(parent_file.c_str(), NULL);
        if (!absolute_path) {
            CONSLOG("get parent file: %s absolute path failed - %d", parent_file.c_str(), errno);
            ret = -errno;
            goto out;
        }

        ret = stat(absolute_path, &stats);
        if (ret) {
            CONSLOG("stat parent file: %s failed - %d", parent_file.c_str(), errno);
            ret = -errno;
            goto out;
        }

        if (!S_ISREG(stats.st_mode)) {
            CONSLOG("parent file: %s is not normal file", parent_file.c_str());
            ret = -EINVAL;
            goto out;
        }

        parent_absolute_win32_path_ = absolute_path;
    } else {
        parent_absolute_win32_path_ = parent_absolute_path;
    }

    if (parent_relative_path.empty()) {
        relative_path = libvdk::file::relative_path_to(file, parent_file, &err);
        if (err) {
            CONSLOG("get parent file: %s relative path failed - %d", parent_file.c_str(), err);
            //ret = err;
            //goto out;
        }
    } else {
        relative_path = parent_relative_path;
    }
    
    parent_linkage_.append("{");
    parent_linkage_.append(linkage);
    parent_linkage_.append("}");

    parent_linkage2_.append("{");
    parent_linkage2_.append(libvdk::guid::toWinString(&libvdk::guid::kNullGuid, false));
    parent_linkage2_.append("}");
    
    if (!relative_path.empty()) {
        parent_relative_path_ = std::move(relative_path);        
    }    

    initParentLocatorHeader();
out:
    return ret;
}

void MetadataSection::initParentLocatorData(size_t parent_locator_table_entry_index) {
    const size_t other_metadata_offset = kMetadataValueOffsetFromTableHeader + sizeof(FileParameters) + 
                sizeof(VirtualDiskSize) + sizeof(VirtualDiskId) + 
                sizeof(LogicalSectorSize) + sizeof(PhysicalSectorSize); 

    const size_t kv_count = parent_locator_with_data_.locator.header.key_value_count;    
    const size_t locator_header_entries_size = sizeof(ParentLocatorHeader) + (kv_count * sizeof(ParentLocatorEntry));    
    
    //std::vector<char> kv_buf;
    //kv_buf.resize(1024, '\0');
    std::string kv_buf;
    kv_buf.reserve(1024);
    size_t kv_total_length = 0;
    // offset is start from ParentLocatorHeader 
    size_t kv_relative_offset = locator_header_entries_size;
    size_t i = 0;
    while (i < kv_count) {
        initParentLocatorEntryKeyValue(kParentLocatorKeys[i], &i, &kv_relative_offset, &kv_buf, &kv_total_length);
    }

    // parent locator
    TableEntry* te = &table_header_entries_.well_known_table_entries_[parent_locator_table_entry_index];
    memcpy(&te->item_id, kParentLocatorGuid, sizeof(te->item_id));
    te->offset = other_metadata_offset;
    te->length = locator_header_entries_size + kv_total_length;
    te->flags = TableEntryFlags::kIsRequired;

    parent_locator_with_data_.data.resize(kv_total_length, '\0');
    memcpy(parent_locator_with_data_.data.data(), kv_buf.data(), kv_total_length);
}

void MetadataSection::initParentLocatorHeader() {
    uint32_t kv_count = 0;    

    if (!parent_linkage_.empty()) {
        ++kv_count;
    }
    if (!parent_linkage2_.empty()) {
        ++kv_count;
    }
    if (!parent_absolute_win32_path_.empty()) {
        ++kv_count;
    }
    if (!parent_relative_path_.empty()) {
        ++kv_count;
    }
    if (!parent_volume_path_.empty()) {
        ++kv_count;
    }

    ParentLocatorHeader* plh = &parent_locator_with_data_.locator.header;
    memcpy(&plh->locator_type_guid, kLocatorTypeGuid, sizeof(kLocatorTypeGuid));
    plh->key_value_count = kv_count;
}

void MetadataSection::initParentLocatorEntryKeyValue(const wchar_t* key, 
        size_t *ple_index, size_t *kv_offset, std::string* buf, size_t* buf_len) {
    
    size_t key_length = 0, value_length = 0;
    std::string init_value;
    if (key == vhdx::metadata::kParentLocatorLinkage) {
        assert(!parent_linkage_.empty());

        key_length = sizeof(vhdx::metadata::kParentLocatorLinkage) - 2;
        buf->append(reinterpret_cast<const char *>(vhdx::metadata::kParentLocatorLinkage), key_length);

        init_value = parent_linkage_;        

    } else if (key == vhdx::metadata::kParentLocatorAbsoluteWin32Path) {
        //assert(!parent_absolute_win32_path_.empty());
        if (!parent_absolute_win32_path_.empty()) {

            key_length = sizeof(vhdx::metadata::kParentLocatorAbsoluteWin32Path) - 2;
            buf->append(reinterpret_cast<const char *>(vhdx::metadata::kParentLocatorAbsoluteWin32Path), key_length);

            init_value = parent_absolute_win32_path_;
        }

    } else if (key == vhdx::metadata::kParentLocatorRelativePath) {
        if (!parent_relative_path_.empty()) {
            key_length = sizeof(vhdx::metadata::kParentLocatorRelativePath) - 2;
            buf->append(reinterpret_cast<const char *>(vhdx::metadata::kParentLocatorRelativePath), key_length);

            init_value = parent_relative_path_;            
        }
    } else if (key == kParentLocatorLinkage2) {
        if (!parent_linkage2_.empty()) {
            key_length = sizeof(vhdx::metadata::kParentLocatorLinkage2) - 2;
            buf->append(reinterpret_cast<const char *>(vhdx::metadata::kParentLocatorLinkage2), key_length);

            init_value = parent_linkage2_;
        }
    } else if (key == vhdx::metadata::kParentLocatorVolumnPath) {
        if (!parent_volume_path_.empty()) {
            key_length = sizeof(vhdx::metadata::kParentLocatorVolumnPath) - 2;
            buf->append(reinterpret_cast<const char *>(vhdx::metadata::kParentLocatorVolumnPath), key_length);

            init_value = parent_volume_path_;
        }
    }

    if (!init_value.empty()) {
        libvdk::convert::Utf8ToUnicodeWrapper uw(init_value);
        value_length = uw.len();
        buf->append(uw.str(), value_length);        

        ParentLocatorEntry* ple = &parent_locator_with_data_.locator.entries[*ple_index];
        ple->key_offset = *kv_offset;
        ple->value_offset = ple->key_offset + key_length;
        ple->key_length = key_length;
        ple->value_length = value_length;

        *kv_offset += key_length + value_length;
        *ple_index += 1;
        *buf_len += key_length + value_length;
    }
}

void MetadataSection::calcBatInfo() {
    chunk_ratio_ = (static_cast<uint64_t>(1 << 23) * logical_sector_size_.size_in_bytes) / 
            file_parameters_.block_size_in_bytes;

    data_block_count_ = libvdk::convert::divRoundUp(virtual_disk_size_.size_in_bytes, file_parameters_.block_size_in_bytes);

    bitmap_block_count_ = libvdk::convert::divRoundUp(data_block_count_, chunk_ratio_);

    if (diskType() == VirtualDiskType::kDifferencing) {
        total_bat_count_ = static_cast<uint64_t>(bitmap_block_count_) * (chunk_ratio_ + 1);
    } else {
        total_bat_count_ = static_cast<uint64_t>(data_block_count_) + 
            (data_block_count_ - 1) / chunk_ratio_;
    }

    sectors_per_block_ = file_parameters_.block_size_in_bytes / logical_sector_size_.size_in_bytes;

    block_size_bits_ = libvdk::convert::ctz32(file_parameters_.block_size_in_bytes);
    logical_sector_size_bits_ = libvdk::convert::ctz32(logical_sector_size_.size_in_bytes);
    chunk_ratio_bits_ = libvdk::convert::ctz32(chunk_ratio_);
    sectors_per_block_bits_ = libvdk::convert::ctz32(sectors_per_block_);
}

int  MetadataSection::parseContent(int fd, uint64_t offset) {
    int ret = 0;

    ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to offset: %" PRIu64 " failed", offset);
        return ret;
    }
       
    ret = libvdk::file::read_file(fd, static_cast<void *>(&table_header_entries_), sizeof(table_header_entries_));
    if (ret) {
        CONSLOG("read metadata header & entries failed");
        return ret;
    }

    TableHeader* th = &table_header_entries_.table_header_;
    if (memcmp(th->signature, kMetadataTableHeaderSignature, sizeof(th->signature)) != 0) {
        CONSLOG("signature mismatch");
        ret = -1;
        return ret;
    }

    TableEntry* te = reinterpret_cast<TableEntry*>(th+1);    
    for (uint32_t i=0; i<th->entry_count; ++i) {
        std::vector<char> pv_buf;
        //std::string pv_buf;
        pv_buf.resize(te->length, 0);

        uint64_t data_offset = offset + te->offset;
        ret = libvdk::file::seek_file(fd, data_offset, SEEK_SET);
        if (ret) {
            CONSLOG("seek to metadata entry[0x%08X] data failed", te->item_id.Data1);
            break;
        }

        ret = libvdk::file::read_file(fd, pv_buf.data(), te->length);
        if (ret) {
            CONSLOG("read metadata entry[0x%08X] data failed", te->item_id.Data1);
            break;
        }
        
        if (memcmp(te->item_id.uuid, kFileParametersGuid, sizeof(te->item_id)) == 0) {                        
            memcpy(&file_parameters_, pv_buf.data(), te->length);
            
        } else if (memcmp(te->item_id.uuid, kVirtualDiskSizeGuid, sizeof(te->item_id)) == 0) {
            memcpy(&virtual_disk_size_, pv_buf.data(), te->length);

        } else if (memcmp(te->item_id.uuid, kVirtualDiskGuid, sizeof(te->item_id)) == 0) {
            memcpy(&virtual_size_guid_, pv_buf.data(), te->length);

        } else if (memcmp(te->item_id.uuid, kLogicalSectorSizeGuid, sizeof(te->item_id)) == 0) {
            memcpy(&logical_sector_size_, pv_buf.data(), te->length);

        } else if (memcmp(te->item_id.uuid, kPhysicalSectorSizeGuid, sizeof(te->item_id)) == 0) {
            memcpy(&physical_sector_size_, pv_buf.data(), te->length);

        } else if (memcmp(te->item_id.uuid, kParentLocatorGuid, sizeof(te->item_id)) == 0) {
            // read all data(ParentLocatorHeader + ParentLocatorEntry + key-value pair entries)
            const char* p = pv_buf.data();
            size_t len = sizeof(parent_locator_with_data_.locator.header);
            memcpy(&parent_locator_with_data_.locator.header, p, len);

            if (memcmp(&parent_locator_with_data_.locator.header.locator_type_guid, 
                        kLocatorTypeGuid, sizeof(kLocatorTypeGuid)) != 0) {
                CONSLOG("parent locator type mismatch");
                ret = -1;
                break;
            }
            p += len;

            len = sizeof(ParentLocatorEntry)*parent_locator_with_data_.locator.header.key_value_count;        
            memcpy(&parent_locator_with_data_.locator.entries, p, len);
            p += len;            

            // read from kv_buf, use offset anymore
            const char* key_pointer = p;
            const char* value_pointer = nullptr;            
            for (uint16_t i=0; i<parent_locator_with_data_.locator.header.key_value_count; ++i) {
                ParentLocatorEntry* ple = &parent_locator_with_data_.locator.entries[i];
                value_pointer = key_pointer + ple->key_length;

                std::vector<uint8_t> value_buf;
                value_buf.resize(ple->value_length + 2, 0);
                memcpy(value_buf.data(), value_pointer, ple->value_length);

                if (memcmp(key_pointer, kParentLocatorLinkage, ple->key_length) == 0) {
                    parent_linkage_ = libvdk::convert::wchar2Utf8(reinterpret_cast<wchar_t*>(value_buf.data()));
                } else if (memcmp(key_pointer, kParentLocatorLinkage2, ple->key_length) == 0) {
                    parent_linkage2_ = libvdk::convert::wchar2Utf8(reinterpret_cast<wchar_t*>(value_buf.data()));
                } else if (memcmp(key_pointer, kParentLocatorRelativePath, ple->key_length) == 0) {
                    parent_relative_path_ = libvdk::convert::wchar2Utf8(reinterpret_cast<wchar_t*>(value_buf.data()));
                } else if (memcmp(key_pointer, kParentLocatorVolumnPath, ple->key_length) == 0) {
                    parent_volume_path_ = libvdk::convert::wchar2Utf8(reinterpret_cast<wchar_t*>(value_buf.data()));
                } else if (memcmp(key_pointer, kParentLocatorAbsoluteWin32Path, ple->key_length) == 0) {
                    parent_absolute_win32_path_ = libvdk::convert::wchar2Utf8(reinterpret_cast<wchar_t*>(value_buf.data()));
                } else {
                    char unknown_key[64] = {'\0'};
                    memcpy(unknown_key, key_pointer, ple->key_length);
                    CONSLOG("unknown locator entry key: %s", 
                        libvdk::convert::wchar2Utf8(reinterpret_cast<wchar_t*>(unknown_key)).c_str());
                }

                key_pointer += ple->key_length + ple->value_length;
            }

            // save locator key-value data
            parent_locator_with_data_.data = std::move(pv_buf);
        }

        te += 1;

    }

    if (!ret) {
        calcBatInfo();
    }

    return ret;
}

int MetadataSection::modifyParentLocator(int fd, uint64_t metadata_offset, 
    const std::string& parent_absolute_path, const std::string& parent_relative_path) {

    int ret = 0;
    uint32_t pl_inner_offset, pl_length; 
    uint64_t pl_entry_offset = 0;
    TableEntry* te = nullptr;
    uint32_t pl_entry_index = 0;
    for (; pl_entry_index<table_header_entries_.table_header_.entry_count; ++pl_entry_index) {
        te = &table_header_entries_.well_known_table_entries_[pl_entry_index];
        if (memcmp(te->item_id.uuid, kParentLocatorGuid, sizeof(te->item_id)) == 0) {
            pl_inner_offset = te->offset;
            pl_length = te->length;
            break;
        }        
    }

    //std::vector<uint8_t> bak_buf(pl_length, '\0');
    uint64_t pl_offset = metadata_offset + pl_inner_offset;
    ret = libvdk::file::seek_file(fd, pl_offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek file to offset: %" PRIu64 "(0x%lx) failed", pl_offset, pl_offset);
        return ret;
    }

    // ret = libvdk::file::read_file(fd, bak_buf.data(), pl_length);
    // if (ret) {
    //     CONSLOG("read file for backup parent locator info failed");        
    // }
    
    std::vector<uint8_t> clear_buf(pl_length, '\0'); 
    ret = libvdk::file::write_file(fd, clear_buf.data(), clear_buf.size());
    if (ret) {
        CONSLOG("write file for clear parent locator info failed");
        return ret;
    }

    if (!parent_absolute_path.empty()) {
        parent_absolute_win32_path_ = parent_absolute_path;
    }
    if (!parent_relative_path.empty()) {
        parent_relative_path_ = parent_relative_path;
    }

    initParentLocatorHeader();
    initParentLocatorData(pl_entry_index);

    pl_entry_offset = metadata_offset + sizeof(TableHeader) + (pl_entry_index * sizeof(TableEntry));
    // rewrite parent locator table entry
    ret = libvdk::file::seek_file(fd, pl_entry_offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek file to offset: %" PRIu64 "(0x%lx) failed", pl_entry_offset, pl_entry_offset);
        return ret;
    }

    ret = libvdk::file::write_file(fd, &table_header_entries_.well_known_table_entries_[pl_entry_index], sizeof(TableEntry));
    if (ret) {
        CONSLOG("write parent locator table entry failed");
        return ret;
    }

    // rewrite parent locator header & data
    ret = libvdk::file::seek_file(fd, pl_offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek file to offset: %" PRIu64 "(0x%lx) failed", pl_offset, pl_offset);
        return ret;
    }

    ret = writeParentLocatorContent(fd);

    return ret;
}

void MetadataSection::show() const {
    printf("=== metadata ===\n");
    printf("block size           : %u\n", file_parameters_.block_size_in_bytes);
    printf("file size            : %" PRIu64 "\n", virtual_disk_size_.size_in_bytes);
    printf("file guid            : %s\n", libvdk::guid::toWinString(&virtual_size_guid_).c_str());
    printf("logical sector size  : %u\n", logical_sector_size_.size_in_bytes);
    printf("physical sector size : %u\n\n", physical_sector_size_.size_in_bytes);

    vhdx::metadata::VirtualDiskType type = diskType();
    printf("disk type            : %s\n\n", (type == vhdx::metadata::VirtualDiskType::kFixed ? "Fixed" : 
        (type == vhdx::metadata::VirtualDiskType::kDynamic ? "Dynamic" : "Differencing")));

    if (type == vhdx::metadata::VirtualDiskType::kDifferencing) {
        printf("linkage              : %s\n", parent_linkage_.c_str());
        printf("linkage2             : %s\n", parent_linkage2_.c_str());
        printf("relative_path        : %s\n", parent_relative_path_.c_str());
        printf("volume_path          : %s\n", parent_volume_path_.c_str());
        printf("absolute_win32_path  : %s\n\n", parent_absolute_win32_path_.c_str());
    }

    printf("chunk ratio          : %u\n", chunk_ratio_);
    printf("data block count     : %u\n", data_block_count_);
    printf("bitmap block count   : %u\n", bitmap_block_count_);
    printf("total bat count      : %u\n\n", total_bat_count_);
}

int  MetadataSection::writeContent(int fd) {
    int ret = 0;
    
    // metadata table header entries
    ret = libvdk::file::seek_file(fd, kMetadataSectionInitOffset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to offset: %u failed", kMetadataSectionInitOffset);
        return ret;
    }    

    ret = libvdk::file::write_file(fd, &table_header_entries_, sizeof(table_header_entries_));
    if (ret) {
        CONSLOG("write metadata table header entry failed");
        return ret;
    }

    ret = libvdk::file::seek_file(fd, kMetadataSectionInitOffset + kMetadataValueOffsetFromTableHeader, SEEK_SET);
    if (ret) {
        CONSLOG("seek to offset: %u failed", kMetadataSectionInitOffset + kMetadataValueOffsetFromTableHeader);
        return ret;
    }

    std::vector<char> value_buf{'\0'};
    value_buf.reserve(128);
    uint32_t value_len = 0;
    char* p = value_buf.data();        
    for (uint32_t i=0; i<table_header_entries_.table_header_.entry_count; ++i) {
        TableEntry *te = &table_header_entries_.well_known_table_entries_[i];
        bool is_parent_locator_entry = false;
        if (memcmp(te->item_id.uuid, kFileParametersGuid, sizeof(te->item_id)) == 0) { 
            memcpy(p, &file_parameters_, te->length);                      
        } else if (memcmp(te->item_id.uuid, kVirtualDiskSizeGuid, sizeof(te->item_id)) == 0) {
            memcpy(p, &virtual_disk_size_, te->length); 
        } else if (memcmp(te->item_id.uuid, kVirtualDiskGuid, sizeof(te->item_id)) == 0) {
            memcpy(p, &virtual_size_guid_, te->length);
        } else if (memcmp(te->item_id.uuid, kLogicalSectorSizeGuid, sizeof(te->item_id)) == 0) {
            memcpy(p, &logical_sector_size_, te->length);            
        } else if (memcmp(te->item_id.uuid, kPhysicalSectorSizeGuid, sizeof(te->item_id)) == 0) {
            memcpy(p, &physical_sector_size_, te->length);
        } else if (memcmp(te->item_id.uuid, kParentLocatorGuid, sizeof(te->item_id)) == 0) {            
            is_parent_locator_entry = true;
        }

        if (!is_parent_locator_entry) {
            p += te->length;
            value_len += te->length;
        }
    }

    ret = libvdk::file::write_file(fd, value_buf.data(), value_len);
    if (ret) {
        CONSLOG("write metadata entry value failed");
        return ret;
    }

    if (diskType() == vhdx::metadata::VirtualDiskType::kDifferencing) {
        ret = writeParentLocatorContent(fd);
    }

    return ret;
}

int MetadataSection::writeParentLocatorContent(int fd) {
    int ret = 0;
    ret = libvdk::file::write_file(fd, 
            &parent_locator_with_data_.locator.header, 
            sizeof(parent_locator_with_data_.locator.header));
    if (ret) {
        CONSLOG("write parent locator header failed");
        return ret;
    }

    ret = libvdk::file::write_file(fd, 
            parent_locator_with_data_.locator.entries,                 
            sizeof(parent_locator_with_data_.locator.entries[0]) * parent_locator_with_data_.locator.header.key_value_count);
    if (ret) {
        CONSLOG("write parent locator entries failed");
        return ret;
    }

    ret = libvdk::file::write_file(fd, 
            parent_locator_with_data_.data.data(),
            parent_locator_with_data_.data.size());
    if (ret) {
        CONSLOG("write parent locator key-value failed");
        return ret;
    }

    return ret;
}

} // namespace metadata
} // namespace vhdx