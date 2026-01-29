#include "vhdx.h"

#include <cassert>
#include <cinttypes>

namespace vhdx {
namespace detail {

#ifndef FALLTHROUGH_INTENDED
#define FALLTHROUGH_INTENDED \
    do {                     \
    } while (0)
#endif

struct SectorInfo {
    uint32_t bat_idx;       /* BAT entry index */
    uint32_t sectors_avail; /* sectors available in payload block */
    uint32_t bytes_left;    /* bytes left in the block after data to r/w */
    uint32_t bytes_avail;   /* bytes available in payload block */
    uint64_t file_offset;   /* absolute offset in bytes, in file */
    uint64_t block_offset;  /* block offset, in bytes */

    uint32_t bitmap_idx;    /* bitmap entry index */
    uint64_t bitmap_offset; /* bitmap offset for differencing, in bytes */
};

#define WRITE_LOG

} // namespace detail

int Vhdx::createVdkFile(const std::string& file, const std::string& parent_file, uint64_t size_in_bytes, 
    bool is_fixed/* = false*/, const std::string& parent_absolute_path, const std::string& parent_relative_path) {
    int ret = 0;
    int fd = 0;
    size_t write_bytes_left = 0;
    uint8_t* p = nullptr;
    uint64_t round_size = libvdk::convert::roundUp(size_in_bytes, libvdk::kMiB);
    uint32_t block_size = 0, logical_sector_size = 0, physicial_sector_size = 0;
    uint64_t file_size = 0UL;
    std::vector<uint8_t> bat_buf;    
    
    header::HeaderSection hdr;
    log::LogSection log;
    metadata::MetadataSection mtd;
    
    vhdx::metadata::VirtualDiskType type;
    if (is_fixed) {
        type = vhdx::metadata::VirtualDiskType::kFixed;
        assert(round_size != 0);
    } else if (!parent_file.empty()) {
        type = vhdx::metadata::VirtualDiskType::kDifferencing;
    } else {
        type = vhdx::metadata::VirtualDiskType::kDynamic;
        assert(round_size != 0);
    }

    // create file first, to make initParentLocatorContent happy
    fd = libvdk::file::create_file(file.c_str());
    if (fd <= 0) {
        CONSLOG("create file: %s failed", file.c_str());
        ret = -1;
        goto end;
    }
    
    if (type == vhdx::metadata::VirtualDiskType::kDifferencing) {
        Vhdx parent_vhdx(parent_file);
        ret = parent_vhdx.parse();
        if (ret) {
            CONSLOG("parse parent file: %s failed", parent_file.c_str());
            goto end;
        }

        ret = mtd.initParentLocatorContent(file, parent_file, 
                libvdk::guid::toWinString(&parent_vhdx.dataWriteGuid(), false),
                parent_absolute_path, parent_relative_path);
        if (ret) {
            CONSLOG("init parent locator with parent file: %s failed", parent_file.c_str());
            goto end;
        }

        // get info from parent vhdx
        round_size = parent_vhdx.diskSize();
        block_size = parent_vhdx.blockSize();
        logical_sector_size = parent_vhdx.logicalSectorSize();
        physicial_sector_size = parent_vhdx.physicalSectorSize();

        assert(round_size != 0);
    }
    mtd.initContent(type, round_size, block_size, logical_sector_size, physicial_sector_size);

    hdr.initContent(mtd.batOccupyMbCount());
    log.initContent(mtd.batOccupyMbCount() + 
        (is_fixed ? (round_size >> libvdk::kMibShift) : 0));    
    
    // write content
    ret = hdr.writeContent(fd);
    if (ret) {
        goto end;
    }
    ret = log.writeContent(fd);
    if (ret) {
        goto end;
    }
    ret = mtd.writeContent(fd);
    if (ret) {
        goto end;
    }

    ret = libvdk::file::seek_file(fd, vhdx::bat::kBatInitOffsetInBytes, SEEK_SET);
    if (ret) {
        CONSLOG("seek to bat offset: %u failed", vhdx::bat::kBatInitOffsetInBytes);
        return ret;
    }
    
    // init & write bat
    bat_buf.resize(mtd.totalBatSizeInBytes(), 0x0);    
    if (is_fixed) {
        vhdx::bat::BatEntry* bat_entries = reinterpret_cast<vhdx::bat::BatEntry *>(bat_buf.data());
        uint64_t payload_offset = vhdx::bat::kBatInitOffsetInBytes + mtd.batOccupySizeInBytes();
        for (uint32_t i=0; i<mtd.totalBatCount(); ++i) {
            *bat_entries = vhdx::bat::makePayloadBatEntry(vhdx::bat::PayloadBatEntryStatus::kBlockFullPresent, payload_offset);

            payload_offset += mtd.blockSize();
            bat_entries += 1;
        }
    }
    
    write_bytes_left = bat_buf.size();
    p = bat_buf.data();
    while (write_bytes_left > 0) {
        size_t write_bytes = 0;
        if (write_bytes_left >= 4 * libvdk::kKiB) {
            write_bytes = 4 * libvdk::kKiB;
        } else {
            write_bytes = write_bytes_left;
        }

        ret = libvdk::file::write_file(fd, p, write_bytes);
        if (ret) {
            CONSLOG("write bat failed - %d", ret);
            break;
        }

        p += write_bytes;
        write_bytes_left -= write_bytes;
    }

    file_size = static_cast<uint64_t>(vhdx::bat::kBatInitOffsetInBytes) + mtd.batOccupySizeInBytes();
    if (is_fixed) {
        file_size += round_size;
    }

    ret = libvdk::file::truncate_file(fd, file_size);
    if (ret) {
        CONSLOG("truncate file: %s to size: %" PRIu64 " failed - %d", file.c_str(), file_size, ret);
    }
    
end:
    if (fd > 0) {
        libvdk::file::close_file(fd);
    }

    if (ret) {
        libvdk::file::delete_file(file);
    }

    return ret;
}

int Vhdx::createDynamic(const std::string& file, uint64_t size_in_bytes) {
    return createVdkFile(file, "", size_in_bytes); 
}

int Vhdx::createDifferencing(const std::string& file, const std::string& parent_file,
    const std::string& parent_absolute_path, const std::string& parent_relative_path) {
    return createVdkFile(file, parent_file, 0UL, false, parent_absolute_path, parent_relative_path);
}

int Vhdx::createFixed(const std::string& file, uint64_t size_in_bytes) {
    return createVdkFile(file, "", size_in_bytes, true);
}

Vhdx::Vhdx()
    : bat_entries_(nullptr),       
      fd_(-1),
      first_visible_write_(true) {

}

Vhdx::Vhdx(const std::string& file, bool read_only/* = true*/) 
    : bat_entries_(nullptr), 
      file_(file), 
      fd_(-1),
      first_visible_write_(true) {
    
    load(file, read_only);
}

Vhdx::~Vhdx() {
    unload();
}

int Vhdx::load(const std::string& file, bool read_only/* = true*/) {
    int ret = 0;
    file_ = file;
    if (read_only) {
        fd_ = libvdk::file::open_file_ro(file.c_str());
        memset(&file_rw_guid_, 0, sizeof(libvdk::guid::GUID));
    } else {
        fd_ = libvdk::file::open_file_rw(file.c_str());
        libvdk::guid::generate(&file_rw_guid_);
    }

    if (fd_ <= 0) {
        ret = -1;
        CONSLOG("open file: %s for %s failed", 
            file.c_str(), (read_only ? "RO" : "RW"));
    }

    return ret;
}

void Vhdx::unload() {
    memset(&hdr_section_, 0, sizeof(hdr_section_));
    memset(&log_section_, 0, sizeof(log_section_));
    memset(&mtd_section_, 0, sizeof(mtd_section_));

    bat_entries_ = nullptr;
    bat_buf_.clear();

    first_visible_write_ = false;

    memset(&file_rw_guid_, 0, sizeof(file_rw_guid_));

    parents_.clear();    

    if (fd_ > 0) {
        libvdk::file::close_file(fd_);
        fd_ = -1;
    }

    file_.clear();
}

int Vhdx::parse() {
    int ret = 0;
    if (fd_ == -1) {
        CONSLOG("file: %s not load", file_.c_str());
        return -1;
    }
    
    if (hdr_section_.parseContent(fd_) != 0) {
        CONSLOG("parse file: %s header section failed", file_.c_str());
        ret = -1;
    } else {
        log_section_.setVhdx(this);
        ret = log_section_.parseContent();
        if (ret) {
            CONSLOG("replay log failed");            
        } else {
            //hs.show();

            // printf("current header index: %d, bat offset: %" PRIu64 ", meta offset: %" PRIu64 "\n", 
            //     hs.getCurrentHeaderIndex(), hs.batEntry().file_offset, hs.metadataEntry().file_offset);

            if (mtd_section_.parseContent(fd_, hdr_section_.metadataEntry().file_offset)) {
                CONSLOG("parse file: %s metadata section failed", file_.c_str());
                ret = -1;
            } 
        }
    }

    if (ret == 0) {
        // read bat
        uint32_t bat_offset = hdr_section_.batEntry().file_offset;
        uint64_t total_bat_size_in_bytes = mtd_section_.totalBatSizeInBytes();
        ret = libvdk::file::seek_file(fd_, bat_offset, SEEK_SET);
        if (ret) {
            CONSLOG("seek to bat offset: %u failed", bat_offset);
            return ret;
        }

        bat_buf_.resize(total_bat_size_in_bytes, '\0');
        ret = libvdk::file::read_file(fd_, bat_buf_.data(), total_bat_size_in_bytes);
        if (ret) {
            CONSLOG("read bat at offset: %u failed", bat_offset);
            return ret;
        }

        bat_entries_ = reinterpret_cast<vhdx::bat::BatEntry*>(bat_buf_.data());
    }

    return ret;
}

int Vhdx::modifyParentLocator(const std::string& parent_absolute_path, const std::string& parent_relative_path) {
    return mtd_section_.modifyParentLocator(fd_, hdr_section_.metadataEntry().file_offset, 
            parent_absolute_path, parent_relative_path);
}

void Vhdx::blockTranslate(uint64_t sector_num, uint32_t nb_sectors, detail::SectorInfo* si) {
    uint32_t block_offset;

    si->bat_idx = sector_num >> mtd_section_.sectorsPerBlockBits();

    /* effectively a modulo - this gives us the offset into the block
     * (in sector sizes) for our sector number */
    block_offset = sector_num - (si->bat_idx << mtd_section_.sectorsPerBlockBits());

    /* the chunk ratio gives us the interleaving of the sector
     * bitmaps, so we need to advance our page block index by the
     * sector bitmaps entry number */
    si->bat_idx += si->bat_idx >> mtd_section_.chunkRatioBits();

    /* the number of sectors we can read/write in this cycle */
    si->sectors_avail = mtd_section_.sectorsPerBlocks() - block_offset;

    si->bytes_left = si->sectors_avail << mtd_section_.logicalSectorSizeBits();

    if (si->sectors_avail > nb_sectors) {
        si->sectors_avail = nb_sectors;
    }

    si->bytes_avail = si->sectors_avail << mtd_section_.logicalSectorSizeBits();
    
    vhdx::bat::payloadBatStatusOffset(bat_entries_[si->bat_idx], nullptr, &si->file_offset);
    
    si->block_offset = block_offset << mtd_section_.logicalSectorSizeBits();

    // //(si->bat_idx + (mtd_section_.chunkRatio() - (si->bat_idx % mtd_section_.chunkRatio()))) + (si->bat_idx >> mtd_section_.chunkRatioBits());    
    uint32_t bat_idx_in_chunk = si->bat_idx >> mtd_section_.chunkRatioBits();
    si->bitmap_idx = ((bat_idx_in_chunk + 1) << mtd_section_.chunkRatioBits()) + bat_idx_in_chunk;
    si->bitmap_offset = 0UL;

    /* The file offset must be past the header section, so must be > 0 */
    if (si->file_offset == 0) {
        return;
    }

    /* block offset is the offset in vhdx logical sectors, in
     * the payload data block. Convert that to a byte offset
     * in the block, and add in the payload data block offset
     * in the file, in bytes, to get the final read address */
    si->file_offset += si->block_offset;     
}

int Vhdx::read(uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf) {
    int ret = 0;

    if (diskType() == vhdx::metadata::VirtualDiskType::kDifferencing) {
        ret = buildParentList();
        if (ret) {
            goto exit;
        }        
    }

    ret = readRecursion(-1, sector_num, nb_sectors, buf);
exit:
    return ret;
}

int Vhdx::readRecursion(int vhdx_index, uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf) {
    using vhdx::bat::PayloadBatEntryStatus;

    int ret = 0;
    detail::SectorInfo si;
    PayloadBatEntryStatus status;    
    //uint64_t bytes_done;
    //CONSLOG("parents size: %lu", parents_.size());
    Vhdx* current_vhdx = nullptr;
    if (!parents_.empty() && vhdx_index >= static_cast<int>(parents_.size())) {
        return 0;
    }

    if (vhdx_index == -1) {
        current_vhdx = this;
    } else {    
        current_vhdx = parents_[vhdx_index].get();
    }

    while (nb_sectors > 0) {        
        current_vhdx->blockTranslate(sector_num, nb_sectors, &si);
        uint64_t offset;
        vhdx::bat::payloadBatStatusOffset(current_vhdx->bat()[si.bat_idx], &status, &offset);

#ifdef RW_DEBUG
        CONSLOG("offset: %" PRIu64 ", status: %s", offset, payloadStatusToString(status));
#endif

        switch (status) {
        case PayloadBatEntryStatus::kBlockNotPresent:
        case PayloadBatEntryStatus::kBlockUndefined:
        case PayloadBatEntryStatus::kBlockUnmapped:
        case PayloadBatEntryStatus::kBlockZero:
            if (current_vhdx->diskType() == vhdx::metadata::VirtualDiskType::kDifferencing) {
                ret = readFromParents(vhdx_index+1, sector_num, nb_sectors, buf);
                if (ret) {
                    CONSLOG("read from parent failed");
                    goto exit;
                }
            } else if (current_vhdx->diskType() == vhdx::metadata::VirtualDiskType::kDynamic) {
                memset(buf, 0, si.bytes_avail);
            } else {
                assert(false);
            }
            break;
        case PayloadBatEntryStatus::kBlockFullPresent:
            ret = current_vhdx->readFromCurrent(si.file_offset, buf, si.bytes_avail);
            if (ret) {
                CONSLOG("read from current failed");
                goto exit;
            }            
            break;
        case PayloadBatEntryStatus::kBlockPartiallyPresent:
            {
                // read bitmap entry
                vhdx::bat::BatEntry bitmap_entry = current_vhdx->bat()[si.bitmap_idx];
                uint64_t bitmap_offset = 0UL;
                vhdx::bat::BitmapBatEntryStatus bitmap_status;
                vhdx::bat::bitmapBatStatusOffset(bitmap_entry, &bitmap_status, &bitmap_offset);

                assert(bitmap_status == vhdx::bat::BitmapBatEntryStatus::kBlockPresent &&
                        bitmap_offset != 0UL);
                
                std::vector<uint8_t> bitmap_buf;
                uint8_t* p;
                uint8_t* tmp_buf;
                uint32_t secs = 0; //sector_num % vhdx::bat::kSectorsPerBitmap;
                uint32_t avail_sectors = 0, unavail_sectors = 0;                
                //ret = current_vhdx->loadBlockBitmap(bitmap_offset, &bitmap_buf);
                ret = current_vhdx->loadPartiallyBlockBitmap(sector_num, si.sectors_avail, &bitmap_offset, &secs, &bitmap_buf);
                if (ret) {
                    CONSLOG("load block bitmap failed");
                    goto exit;
                }

                p = bitmap_buf.data();
                uint64_t partially_sector_num = sector_num;                
                tmp_buf = buf;
                for (uint32_t i=0; i<si.sectors_avail; ++i) {
                    if (testBit(p, secs+i)) {
                        if (unavail_sectors > 0) {
                            uint32_t unavail_bytes = unavail_sectors << current_vhdx->logicalSectorSizeBits();

                            ret = readFromParents(vhdx_index+1, partially_sector_num, unavail_sectors, tmp_buf);
                            if (ret) {
                                CONSLOG("read from parent failed");
                                goto exit;
                            }

                            partially_sector_num += unavail_sectors;
                            tmp_buf += unavail_bytes;

                            unavail_sectors = 0;                                                      
                        }

                        ++avail_sectors;
                    } else {
                        if (avail_sectors > 0) {
                            uint32_t avail_bytes = avail_sectors << current_vhdx->logicalSectorSizeBits();

                            uint64_t avail_offset = si.file_offset + ((partially_sector_num - sector_num) << current_vhdx->logicalSectorSizeBits());

#ifdef RW_DEBUG
                            CONSLOG("read in diff, idx:%d, sector_num: %" PRIu64 ", sectors: %u", 
                                vhdx_index, partially_sector_num, avail_sectors);
#endif                         
                            ret = current_vhdx->readFromCurrent(avail_offset, tmp_buf, avail_bytes);
                            if (ret) {
                                CONSLOG("read from current failed");
                                goto exit;
                            }                            

                            partially_sector_num += avail_sectors;
                            tmp_buf += avail_bytes;

                            avail_sectors = 0;                            
                        }
                        
                        ++unavail_sectors;
                    }
                }

                if (avail_sectors > 0) {
                    uint32_t avail_bytes = avail_sectors << current_vhdx->logicalSectorSizeBits();

                    uint64_t avail_offset = si.file_offset + ((partially_sector_num - sector_num) << current_vhdx->logicalSectorSizeBits());

#ifdef RW_DEBUG
                    CONSLOG("read in diff, idx:%d, sector_num: %" PRIu64 ", sectors: %u", 
                        vhdx_index, partially_sector_num, avail_sectors);
#endif                        

                    ret = current_vhdx->readFromCurrent(avail_offset, tmp_buf, avail_bytes);
                    if (ret) {
                        CONSLOG("read from current failed");
                        goto exit;
                    }                    

                    partially_sector_num += avail_sectors;
                    tmp_buf += avail_bytes;

                    avail_sectors = 0; 
                } else if (unavail_sectors > 0) {
                    uint32_t unavail_bytes = unavail_sectors << current_vhdx->logicalSectorSizeBits();

                    ret = readFromParents(vhdx_index+1, partially_sector_num, unavail_sectors, tmp_buf);
                    if (ret) {
                        CONSLOG("read from parent failed");
                        goto exit;
                    }

                    partially_sector_num += unavail_sectors;
                    tmp_buf += unavail_bytes;

                    unavail_sectors = 0; 
                } else {
                    assert(false);
                }                
            }

            break;
        default:
            ret = -EIO;
            goto exit;
            break;
        }

        sector_num += si.sectors_avail;
        nb_sectors -= si.sectors_avail;        
        buf += si.bytes_avail;          
    }
    ret = 0;
        
exit:
    return ret;
}

int Vhdx::readFromParents(int parents_index, uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf) {
    // uint64_t parent_sector_num = partially_sector_num;
    // uint32_t parent_nb_sectors = unavail_sectors;
    // int v_idx = vhdx_index + 1;

#ifdef RW_DEBUG
    CONSLOG("read recursion, idx:%d, sector_num: %" PRIu64 ", sectors: %u", 
        parents_index, sector_num, nb_sectors);
#endif

    int ret = readRecursion(parents_index, sector_num, nb_sectors, buf);
    if (ret) {
        CONSLOG("recursion read sector: %" PRIu64 " , sectors: %u with parents index: %d failed",
                sector_num, nb_sectors, parents_index);        
    }

    return ret;
}

int Vhdx::readFromCurrent(uint64_t offset, uint8_t* buf, uint32_t len) {
    int ret = libvdk::file::seek_file(fd_, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to %" PRIu64 " failed", offset);
        return ret;
    }
    
    ret = libvdk::file::read_file(fd_, buf, len);
    if (ret) {
        CONSLOG("read from offset %" PRIu64 " with length %u failed", offset, len);        
    }

    return ret;
}

int Vhdx::write(uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf) {
    using vhdx::bat::PayloadBatEntryStatus;

    int ret = -ENOTSUP;
    detail::SectorInfo si;
    vhdx::bat::BatEntry bat_entry, bitmap_bat_entry;
    uint64_t bat_entry_offset, bitmap_bat_entry_offset;
    PayloadBatEntryStatus status;     
    bool bat_update = false, bitmap_bat_update = false, bitmap_update = false; 
    uint64_t bat_prior_offset = 0;
    std::vector<uint8_t> partially_bitmap_buf;   

    ret = userVisibleWrite();
    if (ret) {
        goto exit;
    }

    if (diskType() == vhdx::metadata::VirtualDiskType::kDifferencing) {
        ret = buildParentList();
        if (ret) {
            goto exit;
        }        
    }

    while (nb_sectors > 0) {
        bool use_zero_buffers = false;        
        bool parent_already_alloc_block = false; 
        uint64_t block_partially_present_offset = 0;
        uint64_t partially_bitmap_offset = 0;

        bat_update = bitmap_bat_update = bitmap_update = false;
        
        blockTranslate(sector_num, nb_sectors, &si);
        vhdx::bat::payloadBatStatusOffset(bat_entries_[si.bat_idx], &status, &block_partially_present_offset);

        switch (status) {
        case PayloadBatEntryStatus::kBlockZero:
            /* in this case, we need to preserve zero writes for
                * data that is not part of this write, so we must pad
                * the rest of the buffer to zeroes */
            use_zero_buffers = true;
            FALLTHROUGH_INTENDED;
        case PayloadBatEntryStatus::kBlockNotPresent:
        case PayloadBatEntryStatus::kBlockUndefined:
        case PayloadBatEntryStatus::kBlockUnmapped:
            bat_prior_offset = si.file_offset;

            if (diskType() == vhdx::metadata::VirtualDiskType::kDifferencing) {
                parent_already_alloc_block = isParentAlreadyAllocBlock(si.bat_idx);

#ifdef RW_DEBUG
                CONSLOG("disk type: %d, parent already alloc block: %d", static_cast<int32_t>(diskType()), parent_already_alloc_block);
#endif                
            }

            //bat_prior_offset = si.file_offset;
            ret = allocateBlock(parent_already_alloc_block, &si.file_offset, &si.bitmap_offset, &use_zero_buffers);
            if (ret) {
                goto exit;
            }  

            /*
             * once we support differencing files, this may also be
             * partially present
             */
            if (parent_already_alloc_block) {
                updateBatTablePayloadEntry(si, vhdx::bat::PayloadBatEntryStatus::kBlockPartiallyPresent, &bat_entry, &bat_entry_offset);
                updateBatTableBitmapEntry(si, vhdx::bat::BitmapBatEntryStatus::kBlockPresent, &bitmap_bat_entry, &bitmap_bat_entry_offset);
                bitmap_bat_update = true;
            } else {
                updateBatTablePayloadEntry(si, vhdx::bat::PayloadBatEntryStatus::kBlockFullPresent, &bat_entry, &bat_entry_offset);
            }            

            bat_update = true;

            /*
            * Since we just allocated a block, file_offset is the
            * beginning of the payload block. It needs to be the
            * write address, which includes the offset into the
            * block, unless the entire block needs to read as
            * zeroes but truncation was not able to provide them,
            * in which case we need to fill in the rest.
            */
            si.file_offset += si.block_offset;

            FALLTHROUGH_INTENDED;
        case PayloadBatEntryStatus::kBlockFullPresent:
            /* if the file offset address is in the header zone,
                * there is a problem */
            if (si.file_offset < (1 * libvdk::kMiB)) {
                CONSLOG("write file offset: %" PRIu64 " too small", si.file_offset);
                ret = -EFAULT;
                goto error_bat_restore;
            }

            ret = libvdk::file::seek_file(fd_, si.file_offset, SEEK_SET);
            if (ret) {
                CONSLOG("seek to %" PRIu64 " failed", si.file_offset);
                goto error_bat_restore;
            }
            
            ret = libvdk::file::write_file(fd_, buf, si.bytes_avail);
            if (ret) {
                CONSLOG("write to offset %" PRIu64 " with length %u failed", si.file_offset, si.bytes_avail);
                goto error_bat_restore;
            }           

#ifndef WRITE_LOG
            ret = writeBatTableEntry(si.bat_idx);
            if (ret) {
                CONSLOG("write payload bat entry failed");
                goto exit;
            }

            if (parent_already_alloc_block) {
                ret = writeBitmap(si.bitmap_offset, sector_num, si.sectors_avail);
                if (ret) {
                    CONSLOG("write bitmap failed");
                    goto exit;
                }

                ret = writeBatTableEntry(si.bitmap_idx);
                if (ret) {
                    CONSLOG("write bitmap bat entry failed");
                    goto exit;
                }
            }
#else
            if (parent_already_alloc_block) {
                partially_bitmap_offset = si.bitmap_offset;
                ret = modifyPartiallyBitmap(&partially_bitmap_offset, sector_num, si.sectors_avail, &partially_bitmap_buf);
                if (ret) {
                    CONSLOG("modify partially bitmap failed");
                    goto exit;
                }

                bitmap_update = true;
            }            
#endif            
            
            break;
        case PayloadBatEntryStatus::kBlockPartiallyPresent:            
            assert(block_partially_present_offset != 0UL);
            si.file_offset = block_partially_present_offset + si.block_offset;

            vhdx::bat::BitmapBatEntryStatus bm_status;
            vhdx::bat::bitmapBatStatusOffset(bat_entries_[si.bitmap_idx], &bm_status, &si.bitmap_offset);
            assert(bm_status == vhdx::bat::BitmapBatEntryStatus::kBlockPresent);

            ret = libvdk::file::seek_file(fd_, si.file_offset, SEEK_SET);
            if (ret) {
                CONSLOG("seek to %" PRIu64 " failed", si.file_offset);
                goto exit;
            }
            
            ret = libvdk::file::write_file(fd_, buf, si.bytes_avail);
            if (ret) {
                CONSLOG("write to offset %" PRIu64 " with length %u failed", si.file_offset, si.bytes_avail);
                goto exit;
            }

#ifndef WRITE_LOG
            ret = writeBitmap(si.bitmap_offset, sector_num, si.sectors_avail);
            if (ret) {
                CONSLOG("write bitmap failed");
                goto exit;
            }
#else
            partially_bitmap_offset = si.bitmap_offset;
            ret = modifyPartiallyBitmap(&partially_bitmap_offset, sector_num, si.sectors_avail, &partially_bitmap_buf);
            if (ret) {
                CONSLOG("modify partially bitmap failed");
                goto exit;
            }

            bitmap_update = true;
#endif                       
            break;
        default:
            ret = -EIO;
            goto exit;
            break;
        }

#ifdef WRITE_LOG
        if (bat_update) {
            /* this will update the BAT entry into the log journal, and
                * then flush the log journal out to disk */            
            ret = log_section_.writeLogEntryAndFlush(bat_entry_offset, &bat_entry, sizeof(vhdx::bat::BatEntry));
            if (ret) {
                CONSLOG("write payload bat log entry failed");
                goto exit;
            }
        }

        if (bitmap_update) {
            ret = log_section_.writeLogEntryAndFlush(partially_bitmap_offset, partially_bitmap_buf.data(), partially_bitmap_buf.size());
            if (ret) {
                CONSLOG("write partially bitmap log entry failed");
                goto exit;
            }
        }

        if (bitmap_bat_update) {
            ret = log_section_.writeLogEntryAndFlush(bitmap_bat_entry_offset, &bitmap_bat_entry, sizeof(vhdx::bat::BatEntry));
            if (ret) {
                CONSLOG("write bitmap bat log entry failed");
                goto exit;
            }
        }
#endif
        nb_sectors -= si.sectors_avail;
        sector_num += si.sectors_avail;
        buf += si.bytes_avail;             
    }
    ret = 0;
    goto exit;

error_bat_restore:
    if (bat_update) {
        si.file_offset = bat_prior_offset;
        updateBatTablePayloadEntry(si, status, nullptr, nullptr);
    }

exit:
    return ret;
}

int Vhdx::allocateBlock(bool parent_already_alloc_block, uint64_t* new_offset, uint64_t* bitmap_offset, bool* need_zero) {
    int ret;
    uint64_t current_len, new_file_size;

    ret = libvdk::file::get_file_sizes(fd_, reinterpret_cast<int64_t *>(&current_len));
    if (ret) {
        return ret;
    }    

    *new_offset = current_len;

    *new_offset = libvdk::convert::roundUp(*new_offset, 1 * libvdk::kMiB);

    if (parent_already_alloc_block) {
        *bitmap_offset = *new_offset;
        // added bitmap block size (default 1MiB)
        *new_offset += 1 * libvdk::kMiB;   
    } else {
        *bitmap_offset = 0UL;
    }    

    new_file_size = *new_offset + mtd_section_.blockSize();

    ret = libvdk::file::truncate_file(fd_, new_file_size);
    if (ret) {
        CONSLOG("truncate file: %s to size: %" PRIu64 " failed - %d", file_.c_str(), new_file_size, ret);
    }

    return ret;
}

void Vhdx::updateBatTablePayloadEntry(const detail::SectorInfo& si, vhdx::bat::PayloadBatEntryStatus status, 
            vhdx::bat::BatEntry* bat_entry, uint64_t* bat_entry_offset) {

    bat_entries_[si.bat_idx] = vhdx::bat::makePayloadBatEntry(status, si.file_offset);

    if (bat_entry) {
        *bat_entry = bat_entries_[si.bat_idx];
    }
    if (bat_entry_offset) {
        *bat_entry_offset = hdr_section_.batEntry().file_offset + si.bat_idx * sizeof(vhdx::bat::BatEntry);
    }
}

void Vhdx::updateBatTableBitmapEntry(const detail::SectorInfo& si, vhdx::bat::BitmapBatEntryStatus status,
    vhdx::bat::BatEntry* bat_entry, uint64_t* bat_entry_offset) {

    bat_entries_[si.bitmap_idx] = vhdx::bat::makeBitmapBatEntry(status, si.bitmap_offset); 

    if (bat_entry) {
        *bat_entry = bat_entries_[si.bitmap_idx];
    }

    if (bat_entry_offset) {
        *bat_entry_offset = hdr_section_.batEntry().file_offset + si.bitmap_idx * sizeof(vhdx::bat::BatEntry);
    }   
}

int Vhdx::userVisibleWrite() {
    int ret = 0;
    if (first_visible_write_) {
        first_visible_write_ = false;
        ret = hdr_section_.updateHeader(fd_, &file_rw_guid_);
    }

    return ret;
}

int Vhdx::buildParentList() {
    int ret = 0;
    Vhdx* current = this;
    if (parents_.empty() && diskType() == vhdx::metadata::VirtualDiskType::kDifferencing) {
        while (true) {
            std::string pa_path = current->mtd_section_.parentAbsoluteWin32Path();
            std::string pr_path = current->mtd_section_.parentRelativePath();
            std::string parent_path;
            if (libvdk::file::exist_file(pa_path) == 0) {
                parent_path = pa_path;
            } else if (libvdk::file::exist_file(pr_path) == 0) {
                parent_path = pr_path;
            }
            if (parent_path.empty()) {
                CONSLOG("cannot find parent by %s or %s",
                    pa_path.c_str(), pr_path.c_str());
                ret = -1;
                break;
            }

            Vhdx* parent = new Vhdx(parent_path);
            if (parent->parse()) {
                CONSLOG("parse parent file: %s failed", parent_path.c_str());
                ret = -1;
                break;
            }

            std::string parent_data_write_guid = libvdk::guid::toWinString(&parent->dataWriteGuid(), false);
            std::string current_linkage = current->mtd_section_.parentLinkageForCompare();
            if (parent_data_write_guid != current_linkage) {
                CONSLOG("linkage mismatch[%s|%s]", 
                    current_linkage.c_str(),
                    parent_data_write_guid.c_str());
                ret = -1;
                break;
            }

            parents_.emplace_back(parent);

            if (parent->diskType() != vhdx::metadata::VirtualDiskType::kDifferencing) {
                break;
            }

            current = parent;
        };
    }

    if (ret) {
        parents_.clear();
    }

    //CONSLOG("parent size: %lu", parents_.size());

    return ret;
}

void Vhdx::showParentInfo() {
    printf("=== parent ===\n");
    for (size_t i=0; i<parents_.size(); ++i) {
        std::unique_ptr<Vhdx>& parent = parents_[i];

        parent->showMetadataSection();
    }
}

bool Vhdx::isParentAlreadyAllocBlock(uint32_t bat_index) {
    bool ret = false;
    for (size_t i=0; i<parents_.size(); ++i) {
        std::unique_ptr<Vhdx>& parent = parents_[i];

        vhdx::bat::BatEntry bat_entry = parent->bat()[bat_index];
        vhdx::bat::PayloadBatEntryStatus status;
        vhdx::bat::payloadBatStatusOffset(bat_entry, &status, nullptr);

        if (status == vhdx::bat::PayloadBatEntryStatus::kBlockFullPresent ||
            status == vhdx::bat::PayloadBatEntryStatus::kBlockPartiallyPresent) {
            ret = true;
            break;
        }
    }

    return ret;
}

int Vhdx::loadBlockBitmap(uint64_t bitmap_offset, std::vector<uint8_t>* bitmap_buf) {
    int ret = 0;
    bitmap_buf->resize(1 * libvdk::kMiB);

    ret = libvdk::file::seek_file(fd_, bitmap_offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to %" PRIu64 " failed", bitmap_offset); 
        return ret;       
    }
    
    ret = libvdk::file::read_file(fd_, bitmap_buf->data(), bitmap_buf->size());
    if (ret) {
        CONSLOG("read from offset %" PRIu64 " with length %lu failed", bitmap_offset, bitmap_buf->size());
    }

    return ret;
}

int Vhdx::loadPartiallyBlockBitmap(uint64_t sector_num, uint32_t nb_sectors, 
        uint64_t *bitmap_offset, uint32_t *secs, std::vector<uint8_t>* bitmap_buf) {
    int ret = 0;    

    uint32_t secs_index = sector_num % vhdx::bat::kSectorsPerBitmap;
    uint32_t byte_index = secs_index / 8;
    assert(byte_index < (1 * libvdk::kMiB));
    
    *bitmap_offset += byte_index;    
    *secs = secs_index % 8;

    uint32_t need_bytes = libvdk::convert::divRoundUp((*secs + nb_sectors), 8);
    bitmap_buf->resize(need_bytes);

#ifdef RW_DEBUG
    CONSLOG("sector num: %" PRIu64 ", sectors: %u, load bytes: %u, byte index:[%u:%u], load partially offset: %" PRIu64 "",
        sector_num, nb_sectors, need_bytes, byte_index, *secs, *bitmap_offset);
#endif

    ret = libvdk::file::seek_file(fd_, *bitmap_offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to %" PRIu64 " failed", *bitmap_offset); 
        return ret;       
    }
    
    ret = libvdk::file::read_file(fd_, bitmap_buf->data(), bitmap_buf->size());
    if (ret) {
        CONSLOG("read from offset %" PRIu64 " with length %lu failed", *bitmap_offset, bitmap_buf->size());
    }

    return ret;
}

int Vhdx::saveBlockBitmap(uint64_t bitmap_offset, const std::vector<uint8_t>& bitmap_buf) {
    int ret = 0;    

    ret = libvdk::file::seek_file(fd_, bitmap_offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to %" PRIu64 " failed", bitmap_offset); 
        return ret;       
    }
    
    ret = libvdk::file::write_file(fd_, reinterpret_cast<const void *>(bitmap_buf.data()), bitmap_buf.size());
    if (ret) {
        CONSLOG("write to offset %" PRIu64 " with length %lu failed", bitmap_offset, bitmap_buf.size());
    }

    return ret;
}

int Vhdx::writeBitmap(uint64_t bitmap_offset, uint64_t sector_num, uint32_t nb_sectors) {
    int ret = 0;
    std::vector<uint8_t> bitmap_buf;
    uint8_t *p = nullptr;
    uint32_t secs = 0; //sector_num % vhdx::bat::kSectorsPerBitmap;
    uint32_t i;    

    // ret = loadBlockBitmap(bitmap_offset, &bitmap_buf);
    ret = loadPartiallyBlockBitmap(sector_num, nb_sectors, &bitmap_offset, &secs, &bitmap_buf);
    if (ret) {
        CONSLOG("load block bitmap failed");
        goto exit;
    }

    p = bitmap_buf.data();    
    for (i = 0; i < nb_sectors; ++i) {
        setBit(p, secs + i);
    }

    ret = saveBlockBitmap(bitmap_offset, bitmap_buf);
    if (ret) {
        CONSLOG("save block bitmap failed");
        goto exit;
    }
exit:
    return ret;
}

int Vhdx::modifyPartiallyBitmap(uint64_t *bitmap_offset, uint64_t sector_num, uint32_t nb_sectors, std::vector<uint8_t>* partially_bitmap_buf) {
    int ret = 0;
    uint8_t *p = nullptr;
    uint32_t secs = 0; //sector_num % vhdx::bat::kSectorsPerBitmap;
    uint32_t i;

    ret = loadPartiallyBlockBitmap(sector_num, nb_sectors, bitmap_offset, &secs, partially_bitmap_buf);
    if (ret) {
        CONSLOG("load block bitmap failed");
        goto exit;
    }

    p = partially_bitmap_buf->data();    
    for (i = 0; i < nb_sectors; ++i) {
        setBit(p, secs + i);
    }

exit:
    return ret;
}

int Vhdx::writeBatTableEntry(uint32_t bat_index) {
    int ret = 0;    

    vhdx::bat::BatEntry bat_entry = bat_entries_[bat_index];
    uint64_t bat_entry_offset = hdr_section_.batEntry().file_offset + bat_index * sizeof(vhdx::bat::BatEntry);
    
    ret = libvdk::file::seek_file(fd_, bat_entry_offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to %" PRIu64 " failed", bat_entry_offset); 
        return ret;       
    }
    
    ret = libvdk::file::write_file(fd_, &bat_entry, sizeof(bat_entry));
    if (ret) {
        CONSLOG("write to offset %" PRIu64 " with length %u failed", bat_entry_offset, static_cast<uint32_t>(sizeof(bat_entry)));
    }

    return ret;
}

const char* Vhdx::payloadStatusToString(vhdx::bat::PayloadBatEntryStatus status) {
    const char* ret = "Unknown";
    switch(status) {
    case vhdx::bat::PayloadBatEntryStatus::kBlockNotPresent:
        ret = vhdx::bat::kPayloadNotPresent;
        break;
    case vhdx::bat::PayloadBatEntryStatus::kBlockUndefined:
        ret = vhdx::bat::kPayloadUndefined;
        break;
    case vhdx::bat::PayloadBatEntryStatus::kBlockZero:
        ret = vhdx::bat::kPayloadZero;
        break;
    case vhdx::bat::PayloadBatEntryStatus::kBlockUnmapped:
        ret = vhdx::bat::kPayloadUnmapped;
        break;
    case vhdx::bat::PayloadBatEntryStatus::kBlockFullPresent:
        ret = vhdx::bat::kPayloadFullPresent;
        break;
    case vhdx::bat::PayloadBatEntryStatus::kBlockPartiallyPresent:
        ret = vhdx::bat::kPayloadPartiallyPresent;
        break;
    }
    
    return ret;
}

const char* Vhdx::bitmapStatusToString(vhdx::bat::BitmapBatEntryStatus status) {
    const char* ret = "Unknown";
    switch(status) {
    case vhdx::bat::BitmapBatEntryStatus::kBlockNotPresent:
        ret = vhdx::bat::kBitmapNotPresent;
        break;
    case vhdx::bat::BitmapBatEntryStatus::kBlockPresent:
        ret = vhdx::bat::kBitmapPresent;
        break;
    }

    return ret;
}

} // namespace vhdx