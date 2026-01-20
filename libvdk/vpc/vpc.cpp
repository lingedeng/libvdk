#include "vpc.h"

#include <cinttypes>
#include <cassert>

namespace vpc {

const char kFooterCookie[9] = "conectix";
const uint32_t kFooterFeatures = 0x00000002;
const uint32_t kFileFormatVersion = 0x00010000;
const char kCreatorApp[5] = "vdk";
const uint32_t kCreatorVersion = 0x00000001;
const char kCreatorHostOs[5] = "WORL"; // windows or linux

const char kHeaderCookie[9] = "cxsparse";
const uint32_t kHeaderVersion = 0x00010000;
const uint32_t kPlatformLocatorCodeNone = 0x00000000;
const char kW2ru[5] = "W2ru";
const char kW2ku[5] = "W2ku";

/* VHD uses an epoch of 12:00AM, Jan 1, 2000. This is the Unix timestamp for
 * the start of the VHD epoch. */
#define VHD_EPOCH_START 946684800

struct SectorInfo {
    uint32_t bat_idx;       /* BAT entry index */
    uint32_t sectors_avail; /* sectors available in payload block */
    uint32_t bytes_left;    /* bytes left in the block after data to r/w */
    uint32_t bytes_avail;   /* bytes available in payload block */
    uint64_t file_offset;   /* absolute offset in bytes, in file */
    uint64_t block_offset;  /* block offset, in bytes */

    SectorInfo() 
        : bat_idx(0), 
          sectors_avail(0), 
          bytes_left(0), 
          bytes_avail(0), 
          file_offset(0UL), 
          block_offset(0UL) {

    }
};

int Vpc::createVdkFile(const std::string& file, const std::string& parent_file, uint64_t size_in_bytes, 
        VpcDiskType disk_type, 
        const std::string& parent_absolute_path/* = std::string("")*/, 
        const std::string& parent_relative_path/* = std::string("")*/) {
    int ret = 0;
    uint64_t round_disk_size = 0UL; //libvdk::convert::roundUp(size_in_bytes, 1 * libvdk::kMiB);
    uint64_t total_sectors = 0UL;
    uint32_t max_bat_entries = 0, bat_table_offset = 0;
    uint64_t footer_data_offset = 0xFFFFFFFFFFFFFFFFUL;
    libvdk::convert::Utf8ToUnicodeWrapper pr_path_wrapper, pa_path_wrapper;
    int fd = -1;
    Footer f;    
    Header h;
    std::vector<uint8_t> parent_path_buf, bat_buf;

    // create file first, to make initParentLocatorContent happy
    fd = libvdk::file::create_file(file.c_str());
    if (fd <= 0) {
        CONSLOG("create file: %s failed", file.c_str());
        ret = -1;
        goto end;
    }

    if (size_in_bytes != 0) {
        round_disk_size = libvdk::convert::roundUp(size_in_bytes, 2 * libvdk::kMiB);
    }
    
    memset(&f, 0, sizeof(f));
    memset(&h, 0, sizeof(h));

    if (disk_type != VpcDiskType::kFixed) {
        footer_data_offset = sizeof(Footer);
        bat_table_offset = sizeof(Footer) + sizeof(Header);

        // init header
        if (disk_type == VpcDiskType::kDifferencing) {
            Vpc v(parent_file);
            ret = v.parse();
            if (ret) {
                CONSLOG("parse parent file: %s failed", parent_file.c_str());
                goto end;
            }

            round_disk_size = v.diskSize();            
            memcpy(&h.parent_unique_id, &v.uniqueId(), sizeof(h.parent_unique_id));
            h.parent_timestamp = v.parentTimestamp();
            
            {
                char* parent_name = basename(const_cast<char *>(parent_file.c_str()));
                // save as UTF16-BE
                libvdk::convert::Utf8ToUnicodeWrapper w(parent_name, false);
                memcpy(h.parent_unicode_name, w.str(), w.len());
            }

            int path_err;
            std::string pa_path, pr_path;            
            if (parent_absolute_path.empty()) {
                pa_path = libvdk::file::absolute_path(parent_file, &path_err);
                if (path_err) {
                    CONSLOG("get parent file: %s absolute path failed - %d", parent_file.c_str(), path_err);
                    ret = path_err;
                    goto end;
                }
            } else {
                pa_path = parent_absolute_path;
            }
            pa_path_wrapper.convert(pa_path);
            assert(pa_path_wrapper.str());

            if (parent_relative_path.empty()) {
                pr_path = libvdk::file::relative_path_to(file, parent_file, &path_err);
                if (path_err) {
                    CONSLOG("get parent file: %s relative path failed - %d", parent_file.c_str(), path_err);
                    ret = path_err;
                    goto end;
                }
            } else {
                pr_path = parent_relative_path;
            }
            pr_path_wrapper.convert(pr_path);
            assert(pr_path_wrapper.str());
            
            memcpy(h.parent_locator_entry[0].platform_code, kW2ru, sizeof(h.parent_locator_entry[0].platform_code));
            h.parent_locator_entry[0].platform_data_space = kSectorSize; // 512 bytes                        
            h.parent_locator_entry[0].Platform_data_length = pr_path_wrapper.len();
            h.parent_locator_entry[0].platform_data_offset = bat_table_offset;

            bat_table_offset += h.parent_locator_entry[0].platform_data_space;          

            memcpy(h.parent_locator_entry[1].platform_code, kW2ku, sizeof(h.parent_locator_entry[0].platform_code));
            h.parent_locator_entry[1].platform_data_space = kSectorSize; // 512 bytes            
            h.parent_locator_entry[1].Platform_data_length = pa_path_wrapper.len();
            h.parent_locator_entry[1].platform_data_offset = h.parent_locator_entry[0].platform_data_offset + 
                    h.parent_locator_entry[0].platform_data_space;

            bat_table_offset += h.parent_locator_entry[1].platform_data_space;            
        }

        memcpy(h.cookie, kHeaderCookie, sizeof(h.cookie));
        h.data_offset = 0xFFFFFFFFFFFFFFFFUL;
        h.table_offset = bat_table_offset;
        h.header_version = kHeaderVersion;
        max_bat_entries = (round_disk_size >> kBlockBytesShift);
        h.max_table_entries = max_bat_entries;
        h.block_size = kBlockSize;        

        h.checksum = calcChecksum(&h, sizeof(Header));
        headerOut(&h);
    }    

    // init footer
    total_sectors = (round_disk_size >> kSectorBytesShift);

    memcpy(f.cookie, kFooterCookie, sizeof(f.cookie));
    f.features = kFooterFeatures;
    f.file_format_version = kFileFormatVersion;
    f.data_offset = footer_data_offset;
    f.timestamp = calcTimestamp();
    memcpy(f.creator_app, kCreatorApp, sizeof(f.creator_app));
    f.creator_version = kCreatorVersion;
    memcpy(f.creator_host_os, kCreatorHostOs, sizeof(f.creator_host_os));
    f.original_size = f.current_size = round_disk_size;
    calcDiskGeometry(total_sectors, &f.disk_geometry);
    f.disk_type = static_cast<uint32_t>(disk_type);
    libvdk::guid::generate(&f.unique_id);

    //f.checksum = 0;    
    f.checksum = calcChecksum(&f, sizeof(f));           
    footerOut(&f);

    ret = libvdk::file::seek_file(fd, 0, SEEK_SET);
    if (ret) {
        CONSLOG("seek to file start failed");
        goto end;
    }    

    if (disk_type != VpcDiskType::kFixed) {
        ret = libvdk::file::write_file(fd, &f, sizeof(Footer));
        if (ret) {
            CONSLOG("write footer failed");
            goto end;
        }

        ret = libvdk::file::write_file(fd, &h, sizeof(Header));
        if (ret) {
            CONSLOG("write header failed");
            goto end;
        }


        if (pr_path_wrapper.str()) {
            parent_path_buf.resize(kSectorSize, 0);
            memcpy(parent_path_buf.data(), pr_path_wrapper.str(), pr_path_wrapper.len());

            ret = libvdk::file::write_file(fd, parent_path_buf.data(), parent_path_buf.size());
            if (ret) {
                CONSLOG("write parent relative path failed");
                goto end;
            }
        }
        if (pa_path_wrapper.str()) {
            parent_path_buf.resize(kSectorSize, 0);
            memcpy(parent_path_buf.data(), pa_path_wrapper.str(), pa_path_wrapper.len());

            ret = libvdk::file::write_file(fd, parent_path_buf.data(), parent_path_buf.size());
            if (ret) {
                CONSLOG("write parent absolute path failed");
                goto end;
            }
        }

        // write bat
        uint64_t max_bat_entry_bytes = libvdk::convert::roundUp(max_bat_entries << 2, 512);
        bat_buf.resize(max_bat_entry_bytes, 0xFF);        

        ret = writeBatTable(fd, bat_table_offset, bat_buf.data(), bat_buf.size());
        if (ret) {
            goto end;
        }
    } else {
        ret = libvdk::file::seek_file(fd, round_disk_size, SEEK_CUR);

        if (ret) {
            CONSLOG("seek file: %s to offset: %" PRIu64 " failed - %d", file.c_str(), round_disk_size, ret);
            goto end;
        }
    }

    ret = libvdk::file::write_file(fd, &f, sizeof(Footer));
    if (ret) {
        CONSLOG("write last footer failed");
        goto end;
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

int Vpc::createFixed(const std::string& file, uint64_t size_in_bytes) {
    return createVdkFile(file, "", size_in_bytes, VpcDiskType::kFixed);
}   

int Vpc::createDynamic(const std::string& file, uint64_t size_in_bytes) {
    return createVdkFile(file, "", size_in_bytes, VpcDiskType::kDynamic);
}

int Vpc::createDifferencing(const std::string& file, const std::string& parent_file, 
                const std::string& parent_absolute_path/* = std::string("")*/, 
                const std::string& parent_relative_path/* = std::string("")*/) {    

    return createVdkFile(file, parent_file, 0UL, VpcDiskType::kDifferencing, parent_absolute_path, parent_relative_path);
}

int Vpc::emptyDisk(const std::string& file) {
    int ret = 0;
    std::vector<uint8_t> bat_buf;
    uint64_t max_bat_entry_bytes, new_file_size;    
    uint8_t footer_buf[sizeof(Footer)] = {0};

    Vpc v(file, false);
    if (v.parse(false)) {
        ret = -1;
        goto end;
    }

    if (v.diskType() == VpcDiskType::kFixed) {
        CONSLOG("file: %s type is %s, not support", file.c_str(), v.diskTypeString());
        ret = -ENOTSUP;
        goto end;
    }

    max_bat_entry_bytes = libvdk::convert::roundUp(v.maxBatTableEntries() << 2, 512);
    bat_buf.resize(max_bat_entry_bytes, 0xFF);        

    ret = writeBatTable(v.fd(), v.batTableOffset(), bat_buf.data(), bat_buf.size());
    if (ret) {
        CONSLOG("write bat table failed");
        goto end;
    }    

    memcpy(footer_buf, &v.footer(), sizeof(Footer));
    footerOut(reinterpret_cast<Footer *>(footer_buf));
    ret = libvdk::file::write_file(v.fd(), footer_buf, sizeof(Footer));
    if (ret) {
        CONSLOG("write footer failed");
        goto end;
    }

    new_file_size = v.batTableOffset() + max_bat_entry_bytes + sizeof(Footer);
    ret = libvdk::file::truncate_file(v.fd(), new_file_size);
    if (ret) {
        CONSLOG("truncate file failed");
        goto end;
    }

end:
    return ret;

}

Vpc::Vpc()
    : fd_(-1),
      bat_entries_(nullptr),
      sectors_per_block_(0),
      rewriter_footer_(false) {
    memset(&footer_, 0, sizeof(footer_));
    memset(&header_, 0, sizeof(header_));
}

Vpc::Vpc(const std::string& file, bool read_only/*=true*/)
    : fd_(-1),
      bat_entries_(nullptr),
      sectors_per_block_(0),
      rewriter_footer_(false) {
    memset(&footer_, 0, sizeof(footer_));
    memset(&header_, 0, sizeof(header_));

    load(file, read_only);
}

Vpc::~Vpc() {
    unload();
}

int Vpc::load(const std::string& file, bool read_only/*=true*/) {
    int ret = 0;

    if (fd_ <= 0) {
        file_ = file;
        if (read_only) {
            fd_ = libvdk::file::open_file_ro(file.c_str());        
        } else {
            fd_ = libvdk::file::open_file_rw(file.c_str());        
        }

        if (fd_ <= 0) {
            ret = -1;
            CONSLOG("open file: %s for %s failed", 
                file.c_str(), (read_only ? "RO" : "RW"));
        }
    }

    return ret;
}

void Vpc::unload() {
    int ret = 0;
    if (rewriter_footer_) {
        rewriter_footer_ = false;

        ret = libvdk::file::seek_file(fd_, 0, SEEK_END);
        if (ret) {
            CONSLOG("seek to end file failed");
            goto end;
        }

        footerOut(&footer_);

        ret = libvdk::file::write_file(fd_, &footer_, sizeof(Footer));
        if (ret) {
            CONSLOG("write end file footer failed");
            goto end;
        }        
    }

end:
    memset(&footer_, 0, sizeof(Footer));
    memset(&header_, 0, sizeof(Header));

    bat_entries_ = nullptr;
    bat_buf_.clear();
    sectors_per_block_ = 0;

    parent_absolute_path_.clear();
    parent_relative_path_.clear();
    parents_.clear();

    if (fd_ > 0) {
        libvdk::file::close_file(fd_);
        fd_ = -1;
    }
    file_.clear();
}

int Vpc::parse(bool build_parent_list/*=true*/) {
    int ret = 0;
    uint32_t checksum = 0, calc_chksum = 0;
    if (fd_ <= 0) {
        CONSLOG("file: %s not load", file_.c_str());
        return -1;
    }

    if (memcmp(&footer_.cookie, kFooterCookie, sizeof(footer_.cookie)) == 0) {
        CONSLOG("file: %s alreay parsed", file_.c_str());
        return 0;
    }
        
    int64_t footer_offset = 0;
    bool footer_ok = false;
    ret = libvdk::file::get_file_sizes(fd_, &footer_offset);
    if (ret) {
        CONSLOG("get file size failed, read copy footer");        
    } else {
        footer_offset = footer_offset - sizeof(Footer);
        ret = readFooter(fd_, footer_offset, reinterpret_cast<uint8_t*>(&footer_));
        if (ret) {
            CONSLOG("read footer failed, try copy footer");
            footer_offset = 0;
        } else if (memcmp(footer_.cookie, kFooterCookie, sizeof(footer_.cookie)) != 0) {            
            CONSLOG("file: %s footer cookie mismatch", file_.c_str());
            footer_offset = 0;            
        } else {
            footer_ok = true;
        }
    }

    if (!footer_ok) {
        ret = readFooter(fd_, footer_offset, reinterpret_cast<uint8_t*>(&footer_));
        if (ret) {
            CONSLOG("read copy footer failed");
            goto end;
        }

        if (memcmp(footer_.cookie, kFooterCookie, sizeof(footer_.cookie)) != 0) {
            ret = -1;
            CONSLOG("file: %s copy footer cookie mismatch", file_.c_str());
            goto end;
        } 
    }

    footerIn(&footer_);
    checksum = footer_.checksum;
    footer_.checksum = 0;
    calc_chksum = calcChecksum(&footer_, sizeof(Footer));
    if (checksum != calc_chksum) {
        ret = -1;
        CONSLOG("file: %s footer checksum mismatch(0x%08X|0x%08X)", file_.c_str(), checksum, calc_chksum);
        goto end;
    }
    footer_.checksum = checksum;
        

    if (diskType() != VpcDiskType::kFixed) {
        ret = libvdk::file::seek_file(fd_, footer_.data_offset, SEEK_SET);
        if (ret) {
            CONSLOG("seek to file: %s header failed", file_.c_str());
            goto end;
        }

        ret = libvdk::file::read_file(fd_, &header_, sizeof(Header));
        if (ret) {
            CONSLOG("read file: %s header failed", file_.c_str());
            goto end;
        }

        if (memcmp(header_.cookie, kHeaderCookie, sizeof(header_.cookie)) != 0) {
            ret = -1;
            CONSLOG("file: %s header cookie mismatch", file_.c_str());
            goto end;
        }

        headerIn(&header_);

        checksum = header_.checksum;
        header_.checksum = 0;
        calc_chksum = calcChecksum(&header_, sizeof(Header));
        if (checksum != calc_chksum) {
            ret = -1;
            CONSLOG("file: %s header checksum mismatch(0x%08X|0x%08X)", file_.c_str(), checksum, calc_chksum);
            goto end;
        }
        header_.checksum = checksum;

        if (diskType() == VpcDiskType::kDifferencing) {
            uint32_t data_offset, data_len;
            std::string parent_path;
            for (int i=0; i<8; ++i) {
                const Header::ParentLocatorEntry* ple = &header_.parent_locator_entry[i];

                if (memcmp(ple->platform_code, &kPlatformLocatorCodeNone, sizeof(ple->platform_code)) != 0) {
                    data_offset = ple->platform_data_offset;
                    data_len = ple->Platform_data_length;
                    std::vector<uint8_t> ple_data_buf(data_len+sizeof(wchar_t), 0);                    

                    int ple_ret = libvdk::file::seek_file(fd_, data_offset, SEEK_SET);
                    if (ple_ret) {
                        CONSLOG("seek to file: %s platform locator data with index: %d failed", file_.c_str(), i);
                        continue;
                    }

                    ple_ret = libvdk::file::read_file(fd_, ple_data_buf.data(), data_len);
                    if (ple_ret) {
                        CONSLOG("read file: %s platform locator data with index: %d failed", file_.c_str(), i);
                        continue;
                    }                    

                    if (memcmp(ple->platform_code, kW2ru, sizeof(ple->platform_code)) == 0) {                        
                        parent_relative_path_ = libvdk::convert::wchar2Utf8(reinterpret_cast<wchar_t*>(ple_data_buf.data()));
                    } else if (memcmp(ple->platform_code, kW2ku, sizeof(ple->platform_code)) == 0) {
                        parent_absolute_path_ = libvdk::convert::wchar2Utf8(reinterpret_cast<wchar_t*>(ple_data_buf.data()));
                    }                    
                }
            }

            if (parent_relative_path_.empty() && parent_absolute_path_.empty()) {
                ret = -1;
                CONSLOG("differencing file: %s, not found parent path", file_.c_str());
                goto end;
            }

            if (build_parent_list && buildParentList()) {
                ret = -1;
                goto end;
            }            
        }
        

        // init bat
        sectors_per_block_ = (header_.block_size >> kSectorBytesShift);

        uint64_t max_table_entry_bytes = header_.max_table_entries << 2;
        bat_buf_.resize(max_table_entry_bytes, 0);

        ret = readBatTable(fd_, header_.table_offset, bat_buf_.data(), max_table_entry_bytes);
        if (ret) {
            CONSLOG("read bat table failed");
            goto end;
        }
        bat_entries_ = reinterpret_cast<BatEntry*>(bat_buf_.data());

        for (uint32_t i=0; i<header_.max_table_entries; ++i) {
            libvdk::byteorder::swap32(&bat_entries_[i]);
        }
    }

end:
    return ret;
}

int Vpc::read(uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf) {
    return readRecursion(-1, sector_num, nb_sectors, buf);
}

int Vpc::readRecursion(int32_t parent_index, uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf) {
    int ret = -ENOTSUP;
    SectorInfo si;
    uint64_t bitmap_offset;
    std::vector<uint8_t> bitmap_buf(kBitmapSize, 0);
    Vpc* current = nullptr;

    if (!parents_.empty() && parent_index >= static_cast<int32_t>(parents_.size())) {
        return 0;
    }

    if (parent_index == -1) {
        current = this;
    } else {
        current = parents_[parent_index].get();
    }

    while (nb_sectors > 0) {
        current->blockTranslate(sector_num, nb_sectors, &si);

        if (current->diskType() != VpcDiskType::kFixed) {
            // read bitmap
            BatEntry bentry = current->batTable()[si.bat_idx];
            if (bentry != kBatEntryUnused) {
                bitmap_offset = static_cast<uint64_t>(bentry) << kSectorBytesShift;
                
                ret = readBitmap(current->fd(), bitmap_offset, bitmap_buf.data(), kBitmapSize);
                if (ret) {
                    CONSLOG("sector num: %" PRIu64 ", bat table[%u]: %u, read bitmap failed",
                        sector_num, si.bat_idx, bentry);
                    goto exit;
                }                

                uint8_t* p;
                uint8_t* tmp_buf;
                uint32_t secs = sector_num % kSectorsPerBitmap;
                uint32_t avail_sectors = 0, unavail_sectors = 0;
                uint64_t partially_sector_num = sector_num;                

                p = bitmap_buf.data();                
                tmp_buf = buf;
                for (uint32_t i=0; i<si.sectors_avail; ++i) {
                    if (testBit(p, secs+i)) {
                        if (unavail_sectors > 0) {
                            uint32_t unavail_bytes = unavail_sectors << kSectorBytesShift;

                            if (current->diskType() == VpcDiskType::kDifferencing) {                                
                                // read from parent
                                uint64_t parent_sector_num = partially_sector_num;
                                uint32_t parent_nb_sectors = unavail_sectors;
                                int v_idx = parent_index + 1;

#ifdef RW_DEBUG
                                CONSLOG("read recursion, idx:%d, sector_num: %" PRIu64 ", sectors: %u", 
                                    v_idx, parent_sector_num, parent_nb_sectors);
#endif                                
                                ret = readRecursion(v_idx, parent_sector_num, parent_nb_sectors, tmp_buf);
                                if (ret) {
                                    CONSLOG("recursion read sector: %" PRIu64 " , sectors: %u with parents index: %d failed",
                                            parent_sector_num, parent_nb_sectors, v_idx);
                                    goto exit;
                                }                                
                            } else {
                                memset(tmp_buf, 0, unavail_bytes);
                            }

                            partially_sector_num += unavail_sectors;
                            tmp_buf += unavail_bytes;

                            unavail_sectors = 0;                                                      
                        }

                        ++avail_sectors;
                    } else {
                        if (avail_sectors > 0) {
                            uint32_t avail_bytes = avail_sectors << kSectorBytesShift;

                            uint64_t avail_offset = si.file_offset + ((partially_sector_num - sector_num) << kSectorBytesShift);

#ifdef RW_DEBUG
                            CONSLOG("read in diff, idx:%d, sector_num: %" PRIu64 ", sectors: %u", 
                                parent_index, partially_sector_num, avail_sectors);
#endif                                

                            ret = readPayloadData(current->fd(), avail_offset, tmp_buf, avail_bytes);
                            if (ret) {
                                CONSLOG("read payload failed");
                                goto exit;
                            }
                            // ret = libvdk::file::seek_file(current->fd(), avail_offset, SEEK_SET);
                            // if (ret) {
                            //     CONSLOG("seek to %" PRIu64 " failed", avail_offset);
                            //     goto exit;
                            // }
                            
                            // ret = libvdk::file::read_file(current->fd(), tmp_buf, avail_bytes);
                            // if (ret) {
                            //     CONSLOG("read from offset %" PRIu64 " with length %u failed", si.file_offset, avail_bytes);
                            //     goto exit;
                            // }

                            partially_sector_num += avail_sectors;
                            tmp_buf += avail_bytes;

                            avail_sectors = 0;                            
                        }
                        
                        ++unavail_sectors;
                    }
                }

                if (avail_sectors > 0) {
                    uint32_t avail_bytes = avail_sectors << kSectorBytesShift;

                    uint64_t avail_offset = si.file_offset + ((partially_sector_num - sector_num) << kSectorBytesShift);

#ifdef RW_DEBUG
                    CONSLOG("read in diff, idx:%d, sector_num: %" PRIu64 ", sectors: %u", 
                        parent_index, partially_sector_num, avail_sectors);
#endif                        

                    ret = readPayloadData(current->fd(), avail_offset, tmp_buf, avail_bytes);
                    if (ret) {
                        CONSLOG("read payload failed");
                        goto exit;
                    }

                    partially_sector_num += avail_sectors;
                    tmp_buf += avail_bytes;

                    avail_sectors = 0; 
                } else if (unavail_sectors > 0) {
                    uint32_t unavail_bytes = unavail_sectors << kSectorBytesShift;

                    if (current->diskType() == VpcDiskType::kDifferencing) {                                
                        // read from parent
                        uint64_t parent_sector_num = partially_sector_num;
                        uint32_t parent_nb_sectors = unavail_sectors;
                        int v_idx = parent_index + 1;

#ifdef RW_DEBUG
                        CONSLOG("read recursion, idx:%d, sector_num: %" PRIu64 ", sectors: %u", 
                            v_idx, parent_sector_num, parent_nb_sectors);
#endif                                
                        ret = readRecursion(v_idx, parent_sector_num, parent_nb_sectors, tmp_buf);
                        if (ret) {
                            CONSLOG("recursion read sector: %" PRIu64 " , sectors: %u with parents index: %d failed",
                                    parent_sector_num, parent_nb_sectors, v_idx);
                            goto exit;
                        }                                
                    } else {
                        memset(tmp_buf, 0, unavail_bytes);
                    }

                    partially_sector_num += unavail_sectors;
                    tmp_buf += unavail_bytes;

                    unavail_sectors = 0;
                } else {
                    assert(false);
                }
            } else if (current->diskType() == VpcDiskType::kDifferencing) {
                ret = readRecursion(parent_index+1, sector_num, nb_sectors, buf);
                if (ret) {
                    goto exit;
                }
            } else {
#ifdef RW_DEBUG                
                CONSLOG("Dynamic file: %s, block is not allocated at bat index: %u", current->file().c_str(), si.bat_idx);
#endif                
                memset(buf, 0, si.bytes_avail);
            }
        } else {
            // read block data
            ret = readPayloadData(current->fd(), si.file_offset, buf, si.bytes_avail);
            if (ret) {
                CONSLOG("read fixed payload failed");
                goto exit;
            }            
        }

        sector_num += si.sectors_avail;
        nb_sectors -= si.sectors_avail;
        buf += si.bytes_avail;
    }

    ret = 0;
exit:
    return ret;
}

int Vpc::write(uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf) {
    int ret = -ENOTSUP;
    SectorInfo si;
    uint64_t bitmap_offset;
    BatEntry old_bentry, bentry;
    std::vector<uint8_t> bitmap_buf(kBitmapSize, 0);    

    while (nb_sectors > 0) {
        blockTranslate(sector_num, nb_sectors, &si);

        if (diskType() != VpcDiskType::kFixed) {
            old_bentry = bentry = bat_entries_[si.bat_idx];

            if (bentry == kBatEntryUnused) {
                ret = allocateNewBlock(&si.file_offset);
                if (ret) {
                    goto exit;
                }
                
                bitmap_offset = si.file_offset;
                memset(bitmap_buf.data(), 0, kBitmapSize);

                bentry = bitmap_offset >> kSectorBytesShift;
                bat_entries_[si.bat_idx] = bentry;
                si.file_offset += kBitmapSize + si.block_offset;
            } else {
                bitmap_offset = static_cast<uint64_t>(bentry) << kSectorBytesShift;

                ret = readBitmap(fd_, bitmap_offset, bitmap_buf.data(), kBitmapSize);
                if (ret) {
                    goto exit;
                }                
            }

#ifdef RW_DEBUG
            CONSLOG("write sector num: %" PRIu64 ", bat table[%u]: 0x%08X, bitmap offset: 0x%lX", 
                sector_num, si.bat_idx, bentry, bitmap_offset);
#endif            
            
            { // set bitmap
                uint8_t* p = bitmap_buf.data();
                uint32_t secs = sector_num % kSectorsPerBitmap;
                for (uint64_t i = 0; i < si.sectors_avail; ++i) {
                    setBit(p, secs + i);
                }                
            }            

            // write block data
            ret = writePayloadData(fd_, si.file_offset, buf, si.bytes_avail);
            if (ret) {
                CONSLOG("write payload data failed");
                goto exit;
            }                        

            // write bitmap
            ret = writeBitmap(fd_, bitmap_offset, bitmap_buf.data(), kBitmapSize);
            if (ret) {
                CONSLOG("write bitmap failed");
                goto exit;
            }

            if (old_bentry != bentry) {
                // write bat entry 
                uint64_t bat_entry_offset = header_.table_offset + (si.bat_idx << 2);                

                libvdk::byteorder::swap32(&bentry);

                ret = libvdk::file::seek_file(fd_, bat_entry_offset, SEEK_SET);
                if (ret) {
                    CONSLOG("seek to bat entry offset: %" PRIu64 " failed", bat_entry_offset);
                    goto exit;
                }
                
                ret = libvdk::file::write_file(fd_, &bentry, sizeof(BatEntry));
                if (ret) {
                    CONSLOG("write bat entry to offset %" PRIu64 " failed", bat_entry_offset);
                    goto exit;
                }
            }
        } else {
            // write block data
            ret = writePayloadData(fd_, si.file_offset, buf, si.bytes_avail);
            if (ret) {
                CONSLOG("write payload data failed");
                goto exit;
            }
        }

        sector_num += si.sectors_avail;
        nb_sectors -= si.sectors_avail;
        buf += si.bytes_avail;
    }
    
exit:
    return ret;
}

int Vpc::allocateNewBlock(uint64_t* new_offset) {
    int ret;
    uint64_t current_len, new_file_size;

    ret = libvdk::file::get_file_sizes(fd_, reinterpret_cast<int64_t *>(&current_len));
    if (ret) {
        return ret;
    }    

    if (rewriter_footer_) {
        *new_offset = current_len;        
    } else {
        *new_offset = current_len - sizeof(Footer);
    }

    *new_offset = libvdk::convert::roundUp(*new_offset, 512);        

    // bitmap(512 bytes) + block(2M)
    new_file_size = *new_offset + kBitmapSize + kBlockSize;

    ret = libvdk::file::truncate_file(fd_, new_file_size);
    if (ret) {
        CONSLOG("truncate file: %s to size: %" PRIu64 " failed - %d", file_.c_str(), new_file_size, ret);
    }

    if (!rewriter_footer_) {
        rewriter_footer_ = true;
    }

    return ret;
}

int Vpc::modifyParentLocator(const std::string& pa_path, const std::string& pr_path) {
    int ret = 0;
    uint32_t data_offset, data_space;       
    for (int i=0; i<8; ++i) {
        Header::ParentLocatorEntry* ple = &header_.parent_locator_entry[i];

        if (memcmp(ple->platform_code, &kPlatformLocatorCodeNone, sizeof(ple->platform_code)) != 0) {
            std::string parent_path;

            data_offset = ple->platform_data_offset;            
            data_space = ple->platform_data_space;

            if (memcmp(ple->platform_code, kW2ru, sizeof(ple->platform_code)) == 0 && !pr_path.empty()) {            
                parent_path = pr_path;                        
            } else if (memcmp(ple->platform_code, kW2ku, sizeof(ple->platform_code)) == 0 && !pa_path.empty()) {            
                parent_path = pa_path;
            }            

            if (!parent_path.empty()) {
                libvdk::convert::Utf8ToUnicodeWrapper w(parent_path);
                std::vector<uint8_t> buf(data_space, 0);
                memcpy(buf.data(), w.str(), w.len());

                ret = libvdk::file::seek_file(fd_, data_offset, SEEK_SET);
                if (ret) {
                    CONSLOG("seek to file: %s platform locator data failed", file_.c_str());
                    goto end;
                }

                ret = libvdk::file::write_file(fd_, buf.data(), buf.size());
                if (ret) {
                    CONSLOG("write file: %s platform locator data failed", file_.c_str());
                    goto end;
                }

                // modified data length with new length
                ple->Platform_data_length = w.len();
            }
        }
    }

    // rewrite header
    header_.checksum = 0;
    header_.checksum = calcChecksum(&header_, sizeof(Header));
    headerOut(&header_);

    ret = libvdk::file::seek_file(fd_, sizeof(Footer), SEEK_SET);
    if (ret) {
        CONSLOG("seek to file: %s header failed", file_.c_str());
        goto end;
    }

    ret = libvdk::file::write_file(fd_, &header_, sizeof(Header));
    if (ret) {
        CONSLOG("write file: %s header failed", file_.c_str());
        goto end;
    }

end:
    return ret;
}

int Vpc::buildParentList() {
    int ret = 0;
    Vpc* current = this;
    if (parents_.empty() && diskType() == VpcDiskType::kDifferencing) {
        while (true) {
            std::string pa_path = current->parentAbsolutePath();
            std::string pr_path = current->parentRelativePath();
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

            Vpc* parent = new Vpc(parent_path);
            if (parent->parse()) {
                CONSLOG("parse parent file: %s failed", parent_path.c_str());
                ret = -1;
                break;
            }
            
            if (parent->uniqueId() != current->parentUniqueId()) {
                CONSLOG("parent linkage mismatch[%s|%s]", 
                    libvdk::guid::toWinString(&parent->uniqueId()).c_str(),
                    libvdk::guid::toWinString(&current->parentUniqueId()).c_str());
                ret = -1;
                break;
            }

            parents_.emplace_back(parent);

            if (parent->diskType() != VpcDiskType::kDifferencing) {
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

void Vpc::blockTranslate(uint64_t sector_num, uint32_t nb_sectors, SectorInfo* si) {
    uint32_t block_offset;

    if (diskType() != VpcDiskType::kFixed) {
        si->bat_idx = sector_num / sectors_per_block_;

        /* effectively a modulo - this gives us the offset into the block
        * (in sector sizes) for our sector number */
        block_offset = sector_num % sectors_per_block_;

        /* the number of sectors we can read/write in this cycle */
        si->sectors_avail = sectors_per_block_ - block_offset;

        si->bytes_left = si->sectors_avail << kSectorBytesShift;

        if (si->sectors_avail > nb_sectors) {
            si->sectors_avail = nb_sectors;
        }

        si->bytes_avail = si->sectors_avail << kSectorBytesShift;

        si->block_offset = block_offset << kSectorBytesShift;

        BatEntry bat_entry = bat_entries_[si->bat_idx];
        if (bat_entry == kBatEntryUnused) {
            return;
        }

        si->file_offset = (static_cast<uint64_t>(bat_entry + 1) << kSectorBytesShift) + si->block_offset;
    } else {
        uint64_t max_sectors = footer_.current_size >> kSectorBytesShift;

        if (sector_num >= max_sectors) {
            CONSLOG("sector num: %" PRIu64 " greater or equal than max sector num: %" PRIu64 ", reset sector num",
                sector_num, max_sectors);
            sector_num = max_sectors - 1;
        }

        si->sectors_avail = max_sectors - sector_num;

        //si->bytes_left = si->sectors_avail << kSectorBytesShift;

        if (si->sectors_avail > nb_sectors) {
            si->sectors_avail = nb_sectors;
        }

        si->bytes_avail = si->sectors_avail << kSectorBytesShift;

        si->block_offset = si->file_offset = sector_num << kSectorBytesShift;

        //si->file_offset = si->block_offset;

    }
}

int Vpc::readBatTable(int fd, uint64_t offset, uint8_t* bt_buf, size_t len) {
    int ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to bat table offset: %" PRIu64 " failed", offset);
        return ret;
    }
    ret = libvdk::file::read_file(fd, bt_buf, len);
    if (ret) {
        CONSLOG("read from bat table offset: %" PRIu64 " failed", offset);
    }
    return ret;
}

int Vpc::writeBatTable(int fd, uint64_t offset, const uint8_t* bt_buf, size_t len) {
    int ret = 0;
    size_t write_bytes_left = len;
    const uint8_t *p = bt_buf;

    ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to bat table offset: %" PRIu64 " failed", offset);
        return ret;
    }

    while (write_bytes_left > 0) {
        size_t write_bytes = 0;
        if (write_bytes_left >= 4 * libvdk::kKiB) {
            write_bytes = 4 * libvdk::kKiB;
        } else {
            write_bytes = write_bytes_left;
        }

        ret = libvdk::file::write_file(fd, p, write_bytes);
        if (ret) {
            CONSLOG("write bat table failed - %d", ret);
            break;
        }

        p += write_bytes;
        write_bytes_left -= write_bytes;
    }

    return ret;
}

int Vpc::readBitmap(int fd, uint64_t offset, uint8_t* bm_buf, size_t len) {
    int ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to bitmap offset: %" PRIu64 " failed", offset);
        return ret;
    }
    ret = libvdk::file::read_file(fd, bm_buf, len);
    if (ret) {
        CONSLOG("read from bitmap offset: %" PRIu64 " failed", offset);        
    }
    return ret;
}

int Vpc::writeBitmap(int fd, uint64_t offset, const uint8_t* bm_buf, size_t len) {
    int ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to bitmap offset: %" PRIu64 " failed", offset);
        return ret;
    }
    
    ret = libvdk::file::write_file(fd, bm_buf, len);
    if (ret) {
        CONSLOG("write to bitmap offset %" PRIu64 " with length %lu failed", offset, len);
    }

    return ret;
}

int Vpc::readPayloadData(int fd, uint64_t offset, uint8_t* pld_buf, size_t len) {
    int ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to payload data offset: %" PRIu64 " failed", offset);
        return ret;
    }
    ret = libvdk::file::read_file(fd, pld_buf, len);
    if (ret) {
        CONSLOG("read from payload data offset: %" PRIu64 " failed", offset);        
    }
    return ret;
}

int Vpc::writePayloadData(int fd, uint64_t offset, const uint8_t* pld_buf, size_t len) {
    int ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to payload data offset: %" PRIu64 " failed", offset);
        return ret;
    }
    
    ret = libvdk::file::write_file(fd, pld_buf, len);
    if (ret) {
        CONSLOG("write to payload data offset %" PRIu64 " with length %lu failed", offset, len);
    }

    return ret;
}

int Vpc::readFooter(int fd, uint64_t offset, uint8_t* f_buf) {
    int ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to footer offset: %" PRIu64 " failed", offset);
        return ret;
    }
    ret = libvdk::file::read_file(fd, f_buf, sizeof(Footer));
    if (ret) {
        CONSLOG("read from footer offset: %" PRIu64 " failed", offset);        
    }
    return ret;
}

int Vpc::writeFooter(int fd, uint64_t offset, const uint8_t* f_buf) {
    int ret = libvdk::file::seek_file(fd, offset, SEEK_SET);
    if (ret) {
        CONSLOG("seek to footer offset: %" PRIu64 " failed", offset);
        return ret;
    }
    ret = libvdk::file::write_file(fd, f_buf, sizeof(Footer));
    if (ret) {
        CONSLOG("write to footer offset: %" PRIu64 " failed", offset);        
    }
    return ret;
}


uint32_t Vpc::calcTimestamp() {
    return time(NULL) - VHD_EPOCH_START;
}

uint32_t Vpc::calcChecksum(const void* data, size_t len) {
    return libvdk::encrypt::checksum(reinterpret_cast<const uint8_t*>(data), len);
}

void Vpc::calcDiskGeometry(uint64_t total_sectors, Footer::DiskGeometry* dg) {
    if (total_sectors > 65535 * 16 * 255) {
			total_sectors = 65535 * 16 * 255;
	}

    uint32_t cylinder_times_heads = 0;
    if (total_sectors >= 65535 * 16 * 63) {
        dg->sectors_per_track = 255;
        dg->heads = 16;
        cylinder_times_heads = total_sectors / dg->sectors_per_track;
    }
    else
    {
        dg->sectors_per_track = 17; 
        cylinder_times_heads = total_sectors / dg->sectors_per_track;

        dg->heads = static_cast<uint8_t>((cylinder_times_heads + 1023) / 1024);

        if (dg->heads < 4) {
            dg->heads = 4;
        }
        if (cylinder_times_heads >= (static_cast<uint32_t>(dg->heads) * 1024) || 
            dg->heads > static_cast<uint8_t>(16)) {

            dg->sectors_per_track = 31;
            dg->heads = 16;
            cylinder_times_heads = total_sectors / dg->sectors_per_track;	
        }
        if ( cylinder_times_heads >= (static_cast<uint32_t>(dg->heads) * 1024))
        {
            dg->sectors_per_track = 63;
            dg->heads = 16;
            cylinder_times_heads = total_sectors / dg->sectors_per_track;
        }
    }

    dg->cylinder = static_cast<uint16_t>(cylinder_times_heads / dg->heads);
}

void Vpc::footerByteOrderSwap(Footer* f) {
    libvdk::byteorder::swap32(&f->features);
    libvdk::byteorder::swap32(&f->file_format_version);
    libvdk::byteorder::swap64(&f->data_offset);
    libvdk::byteorder::swap32(&f->timestamp);
    libvdk::byteorder::swap32(&f->creator_version);    
    libvdk::byteorder::swap64(&f->original_size);
    libvdk::byteorder::swap64(&f->current_size);
    libvdk::byteorder::swap16(&f->disk_geometry.cylinder);
    libvdk::byteorder::swap32(&f->disk_type);
    libvdk::byteorder::swap32(&f->checksum);
}

void Vpc::headerByteOrderSwap(Header* h) {
    libvdk::byteorder::swap64(&h->data_offset);
    libvdk::byteorder::swap64(&h->table_offset);
    libvdk::byteorder::swap32(&h->header_version);
    libvdk::byteorder::swap32(&h->max_table_entries);
    libvdk::byteorder::swap32(&h->block_size);
    libvdk::byteorder::swap32(&h->checksum);
    libvdk::byteorder::swap32(&h->parent_timestamp);

    for (int i=0; i<8; ++i) {
        //libvdk::byteorder::swap32(&h->parent_locator_entry[i].platform_code);
        libvdk::byteorder::swap32(&h->parent_locator_entry[i].platform_data_space);
        libvdk::byteorder::swap32(&h->parent_locator_entry[i].Platform_data_length);
        libvdk::byteorder::swap64(&h->parent_locator_entry[i].platform_data_offset);
    }
}

void Vpc::show() const {
    printf("=== Footer ===\n--------------\n");
    printf("cookie              : %s\n", kFooterCookie);
    printf("features            : 0x%08X\n", footer_.features);
    printf("file format version : 0x%08X\n", footer_.file_format_version);
    printf("data offset         : %" PRIu64 " (0x%08lX)\n", footer_.data_offset, footer_.data_offset);
    printf("timestamp           : 0x%08X\n", footer_.timestamp);
    printf("creator app         : %s\n", footer_.creator_app);
    printf("creator version     : 0x%08X\n", footer_.creator_version);
    printf("creator host os     : %s\n", kCreatorHostOs);
    printf("original size       : %" PRIu64 " (0x%lX)\n", footer_.original_size, footer_.original_size);    
    printf("current size        : %" PRIu64 " (0x%lX)\n", footer_.current_size, footer_.current_size);    
    printf("CHS                 : c: %d, h: %d, s: %d\n", 
        footer_.disk_geometry.cylinder, footer_.disk_geometry.heads, footer_.disk_geometry.sectors_per_track);
    printf("disk type           : %s\n", diskTypeString());
    printf("checksum            : 0x%08X\n", footer_.checksum);
    printf("disk uuid           : %s\n", libvdk::guid::toWinString(&footer_.unique_id).c_str());    

    if (diskType() != VpcDiskType::kFixed) {
        printf("\n=== Header ===\n--------------\n");
        printf("cookie            : %s\n", kHeaderCookie);
        printf("data offset       : 0x%016lX\n", header_.data_offset);
        printf("table offset      : %" PRIu64 " (0x%08lX)\n", header_.table_offset, header_.table_offset);
        printf("header version    : 0x%08X\n", header_.header_version);
        printf("max table entries : %u (0x%08X)\n", header_.max_table_entries, header_.max_table_entries);        
        printf("block size        : %u (0x%08X)\n", header_.block_size, header_.block_size);
        printf("checksum          : 0x%08X\n", header_.checksum);
        printf("parent disk uuid  : %s\n", libvdk::guid::toWinString(&header_.parent_unique_id).c_str());
        printf("parent timestamp  : 0x%08X\n", header_.parent_timestamp);
        printf("parent disk name  : %s\n", 
                libvdk::convert::wchar2Utf8(reinterpret_cast<const wchar_t*>(header_.parent_unicode_name), false).c_str());

        if (diskType() == VpcDiskType::kDifferencing) {                        
            // printf("parent relative path: %s\n", parent_relative_path_.c_str());
            // printf("parent absolute path: %s\n", parent_absolute_path_.c_str());
            printf("\n=== Parent locator ===\n----------------------\n");
            uint32_t data_offset, data_len;
            std::string parent_path;
            for (int i=0; i<8; ++i) {
                const Header::ParentLocatorEntry* ple = &header_.parent_locator_entry[i];

                if (memcmp(ple->platform_code, &kPlatformLocatorCodeNone, sizeof(ple->platform_code)) != 0) {
                    data_offset = ple->platform_data_offset;
                    data_len = ple->Platform_data_length;                    

                    printf("locator : %d\n", i);

                    if (memcmp(ple->platform_code, kW2ru, sizeof(ple->platform_code)) == 0) {
                        printf("\tdata code    : %s\n", kW2ru);
                        printf("\tdata value   : %s\n", parent_relative_path_.c_str());
                    } else if (memcmp(ple->platform_code, kW2ku, sizeof(ple->platform_code)) == 0) {
                        printf("\tdata code    : %s\n", kW2ku);
                        printf("\tdata value   : %s\n", parent_absolute_path_.c_str());                        
                    } else {
                        printf("\tdata code    : 0x%08X (Not Support)\n", *(reinterpret_cast<const uint32_t*>(ple->platform_code)));
                    }
                    printf("\tdata space   : %u (0x%08X)\n", ple->platform_data_space, ple->platform_data_space);
                    printf("\tdata lengeth : %u (0x%08X)\n", data_len, data_len);
                    printf("\tdata offset  : %u (0x%08X)\n", data_offset, data_offset);                    
                }
            }
            printf("\n");
        }
    }
}

int Vpc::readBatEntryBitmap(uint64_t sector_num, BatEntry* bentry, uint8_t* buf) {
    int ret = 0;
    uint32_t bat_idx = sector_num / sectors_per_block_;
    *bentry = bat_entries_[bat_idx];

    if (*bentry != kBatEntryUnused) {
        uint64_t offset = static_cast<uint64_t>(*bentry) << kSectorBytesShift;

        ret = readBitmap(fd_, offset, buf, sizeof(kBitmapSize));        
    }

    return ret;
}

} // namespace vpc