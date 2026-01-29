#ifndef LIBVDK_VHDX_VHDX_H_
#define LIBVDK_VHDX_VHDX_H_

#include <memory>
#include <vector>

#include "common.h"
#include "utils.h"

#include "header.h"
#include "log.h"
#include "metadata.h"

namespace vhdx {
namespace detail {
struct SectorInfo;
} // namespace detail

class Vhdx {
public:
    static int createFixed(const std::string& file, uint64_t size_in_bytes);    
    static int createDynamic(const std::string& file, uint64_t size_in_bytes);
    static int createDifferencing(const std::string& file, const std::string& parent_file, 
                const std::string& parent_absolute_path = std::string(""), 
                const std::string& parent_relative_path = std::string(""));

    Vhdx();
    explicit Vhdx(const std::string& file, bool read_only = true);
    Vhdx(const Vhdx& rhs) = delete;
    Vhdx& operator=(const Vhdx& rhs) = delete;

    ~Vhdx();

    int load(const std::string& file, bool read_only = true);
    void unload();

    int parse();

    int modifyParentLocator(const std::string& parent_absolute_path, const std::string& parent_relative_path);

    int read(uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf);
    int write(uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf);

    int fd() const {
        return fd_;
    }

    header::HeaderSection* headerSection() {
        return &hdr_section_;
    }

    const libvdk::guid::GUID& dataWriteGuid() {
        //return hdr_section_.header(hdr_section_.getCurrentHeaderIndex()).data_write_guid;
        return hdr_section_.activeHeaderDataWriteGuid();
    }

    vhdx::metadata::VirtualDiskType diskType() const {
        return mtd_section_.diskType();
    }    

    uint32_t blockSize() const {
        return mtd_section_.blockSize();
    }

    uint64_t diskSize() const {
        return mtd_section_.diskSize();
    }

    uint32_t logicalSectorSize() const {
        return mtd_section_.logicalSectorSize();
    }

    uint32_t physicalSectorSize() const {
        return mtd_section_.physicalSectorSize();
    }

    uint32_t chunkRatio() const {
        return mtd_section_.chunkRatio();
    }    

    uint32_t dataBlockCount() const {
        return mtd_section_.dataBlockCount();
    }

    uint32_t bitmapBlockCount() const {
        return mtd_section_.bitmapBlockCount();
    }

    uint32_t totalBatCount() const {
        return mtd_section_.totalBatCount();
    }

    const std::string& parentLinkage() const {
        return mtd_section_.parentLinkage();
    }    
    const std::string& parentLinkage2() const {
        return mtd_section_.parentLinkage2();
    }
    const std::string parentRelativePath() const {
        return mtd_section_.parentRelativePath();
    }
    const std::string parentVolumePath() const {
        return mtd_section_.parentVolumePath();
    }    
    const std::string parentAbsoluteWin32Path() const {
        return mtd_section_.parentAbsoluteWin32Path();
    }

    uint32_t sectorsPerBlocks() const {
        return mtd_section_.sectorsPerBlocks();
    }

    uint32_t blockSizeBits() const {
        return mtd_section_.blockSizeBits();
    }

    uint32_t logicalSectorSizeBits() const {
        return mtd_section_.logicalSectorSizeBits();
    }

    uint32_t chunkRatioBits() const {
        return mtd_section_.chunkRatioBits();
    }

    uint32_t sectorsPerBlockBits() const {
        return mtd_section_.sectorsPerBlockBits();
    }

    const vhdx::bat::BatEntry* bat() const {
        return bat_entries_;
    }    
    
    void showHeaderSection() const {
        hdr_section_.show();
    }
    void showMetadataSection() const {
        mtd_section_.show();
    }

    void showLogEntries() {
        log_section_.show();
    }

    void showParentInfo();

    int buildParentList();
    bool isParentAlreadyAllocBlock(uint32_t bat_index);

    /* Per the spec, on the first write of guest-visible data to the file the
     * data write guid must be updated in the header */
    int userVisibleWrite(); 

    static const char* payloadStatusToString(vhdx::bat::PayloadBatEntryStatus status);
    static const char* bitmapStatusToString(vhdx::bat::BitmapBatEntryStatus status);
private:
    const uint8_t kBitMask = 0x80;

    bool testBit(uint8_t *addr, int nr) {
	    return ((addr[nr >> 3] << (nr & 7)) & kBitMask) != 0;
    }

    void setBit(uint8_t *addr, int nr) {
        addr[nr >> 3] |= (kBitMask >> (nr & 7));
    }

    void clearBit(uint8_t *addr, int nr)
    {
        addr[nr >> 3] &= ~(kBitMask >> (nr & 7));
    }

    static int createVdkFile(const std::string& file, const std::string& parent_file, uint64_t size_in_bytes, 
        bool is_fixed = false, 
        const std::string& parent_absolute_path = std::string(""), 
        const std::string& parent_relative_path = std::string(""));

    // Perform sector to block offset translations, to get various sector and file offsets into the image.
    void blockTranslate(uint64_t sector_num, uint32_t nb_sectors, detail::SectorInfo* si);

    int  allocateBlock(bool parent_already_alloc_block, uint64_t* new_offset, uint64_t* bitmap_offset, bool* need_zero);
    void updateBatTablePayloadEntry(const detail::SectorInfo& si, vhdx::bat::PayloadBatEntryStatus status, 
            vhdx::bat::BatEntry* bat_entry, uint64_t* bat_entry_offset);
    void updateBatTableBitmapEntry(const detail::SectorInfo& si, vhdx::bat::BitmapBatEntryStatus status,
            vhdx::bat::BatEntry* bat_entry, uint64_t* bat_entry_offset);

    int writeBitmap(uint64_t bitmap_offset, uint64_t sector_num, uint32_t nb_sectors);
    int loadBlockBitmap(uint64_t bitmap_offset, std::vector<uint8_t>* bitmap_buf);
    int saveBlockBitmap(uint64_t bitmap_offset, const std::vector<uint8_t>& bitmap_buf);
    int loadPartiallyBlockBitmap(uint64_t sector_num, uint32_t nb_sectors, 
            uint64_t *bitmap_offset, uint32_t *secs, std::vector<uint8_t>* bitmap_buf);
    int modifyPartiallyBitmap(uint64_t *bitmap_offset, uint64_t sector_num, uint32_t nb_sectors, 
            std::vector<uint8_t>* partially_bitmap_buf);

    int writeBatTableEntry(uint32_t bat_index);

    int readRecursion(int vhdx_index, uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf);
    int readFromParents(int parents_index, uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf);
    int readFromCurrent(uint64_t offset, uint8_t* buf, uint32_t len);

    header::HeaderSection hdr_section_;
    log::LogSection log_section_;
    metadata::MetadataSection mtd_section_;

    std::vector<uint8_t> bat_buf_;
    // point to the begin of bat_buf_.data()
    vhdx::bat::BatEntry* bat_entries_;

    std::string file_;
    int fd_;

    bool first_visible_write_;
    /* This is used for any header updates, for the file_write_guid.
     * The spec dictates that a new value should be used for the first
     * header update */
    libvdk::guid::GUID file_rw_guid_;

    std::vector<std::unique_ptr<Vhdx>> parents_;
};
} //namespace vhdx

#endif