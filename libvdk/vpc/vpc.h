#ifndef LIBVDK_VPC_VPC_H_
#define LIBVDK_VPC_VPC_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include "utils.h"

namespace vpc {
/*
 * All values in the file format, unless otherwise specified, are stored in network byte order (big endian). 
 * Also, unless otherwise specified, all reserved values should be set to zero.
 */
#pragma pack(push, 1)

struct Footer {
	char			    cookie[8];              // “conectix”
	uint32_t		    features;               // No features enabled(0x00000000)
	uint32_t		    file_format_version;    // this field must be initialized to 0x00010000
	uint64_t	        data_offset;            // This field holds the absolute byte offset, from the beginning of the file, to the next structure
                                                // This field is used for dynamic disks and differencing disks, but not fixed disks
                                                // For fixed disks, this field should be set to 0xFFFFFFFF (kFixed without header)
	uint32_t		    timestamp;              // This field stores the creation time of a hard disk image
                                                // This is the number of seconds since January 1, 2000 12:00:00 AM in UTC/GMT
	char			    creator_app[4];
	uint32_t		    creator_version;
	char		        creator_host_os[4];     // 0x5769326B (Wi2k)
	uint64_t	        original_size;          // This field stores the size of the hard disk in bytes,
	uint64_t            current_size;           // This field stores the current size of the hard disk
	struct DiskGeometry
	{
		uint16_t	    cylinder;
		uint8_t		    heads;
		uint8_t		    sectors_per_track;
	} disk_geometry;
	uint32_t		    disk_type;              // see VpcDiskType
	uint32_t		    checksum;
	libvdk::guid::GUID	unique_id;              // Every hard disk has a unique ID stored in the hard disk
	uint8_t			    saved_state;            // This field holds a one-byte flag that describes whether the system is in saved state
    char                reserved[427];	
};

struct Header {
	char			        cookie[8];              // "cxsparse"
	uint64_t	            data_offset;            // It is currently unused by existing formats and should be set to 0xFFFFFFFF
	uint64_t	            table_offset;           // This field stores the absolute byte offset of 
                                                    // the Block Allocation Table (BAT) in the file.
	uint32_t		        header_version;         // this field must be initialized to 0x00010000
	uint32_t		        max_table_entries;      // This field holds the maximum entries present in the BAT
	uint32_t		        block_size;             // A block is a unit of expansion for dynamic and differencing hard disks.
                                                    // It is stored in bytes. This size does not include the size of the block bitmap
                                                    // The sectors per block must always be a power of two. 
                                                    // The default value is 0x00200000
	uint32_t		        checksum;
	libvdk::guid::GUID      parent_unique_id;       // This field is used for differencing hard disks
	uint32_t        		parent_timestamp;       // This field stores the modification time stamp of the parent hard disk
	uint32_t			    reserved1;
	char			        parent_unicode_name[512];  // This field contains a Unicode string (UTF-16) of the parent hard disk filename
	struct ParentLocatorEntry
	{
		char		        platform_code[4];           // see VpcPlatformCode
		uint32_t			platform_data_space;        // This field stores the number of 512-byte sectors 
                                                        // needed to store the parent hard disk locator
		uint32_t			Platform_data_length;       // This field stores the actual length of the parent hard disk locator in bytes
		uint32_t			reserved;
		uint64_t	        platform_data_offset;       // This field stores the absolute file offset in bytes where 
                                                        // the platform specific file locator data is stored
	} parent_locator_entry[8];
	char			        reserved2[256];
};

#pragma pack(pop)

const uint32_t kSectorBytesShift = 9;
const uint32_t kSectorSize = (1 << kSectorBytesShift);

const uint32_t kBlockBytesShift = 21;
const uint32_t kBlockSize = (1 << kBlockBytesShift);

const uint32_t kBitmapSize = kSectorSize;
const uint32_t kSectorsPerBitmap = (kSectorSize << 3);

const uint64_t kMaxSectors = ((2 * libvdk::kTiB) >> kSectorBytesShift);
const uint32_t kMaxBatEntryCount = ((2 * libvdk::kTiB) >> kBlockBytesShift);
const uint32_t kMaxBatTableSize = (kMaxBatEntryCount << 2);


enum class VpcDiskType : uint32_t {
    kFixed = 2,
    kDynamic = 3,
    kDifferencing = 4,
};

using BatEntry = uint32_t;
const uint32_t kBatEntryUnused = 0xFFFFFFFF;
struct SectorInfo;

class Vpc {
public:
    static int createFixed(const std::string& file, uint64_t size_in_bytes);    
    static int createDynamic(const std::string& file, uint64_t size_in_bytes);
    static int createDifferencing(const std::string& file, const std::string& parent_file, 
                const std::string& parent_absolute_path = std::string(""), 
                const std::string& parent_relative_path = std::string(""));
    static int emptyDisk(const std::string& file);

    Vpc();
    explicit Vpc(const std::string& file, bool read_only=true);
    ~Vpc();

    Vpc(const Vpc& rhs) = delete;
    Vpc& operator=(const Vpc& rhs) = delete;

    int load(const std::string& file, bool read_only=true);
    int parse(bool build_parent_list=true);
    void unload();

    int modifyParentLocator(const std::string& pa_path, const std::string& pr_path);

    int read(uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf);
    int write(uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf);

    const std::string& file() const {
        return file_;
    }

    int fd() const {
        return fd_;
    }    

    VpcDiskType diskType() const {
        return static_cast<VpcDiskType>(footer_.disk_type);
    }

    const char* diskTypeString() const {
        const char* dts = "Unknown";
        switch(diskType()) {
            case VpcDiskType::kFixed:
                dts = "Fixed";
                break;
            case VpcDiskType::kDynamic:
                dts = "Dynamic";
                break;
            case VpcDiskType::kDifferencing:
                dts = "Differencing";
                break;
        }
        return dts;
    }

    uint64_t diskSize() const {
        return footer_.current_size;
    }

    const libvdk::guid::GUID& uniqueId() const {
        return footer_.unique_id;
    }
    const libvdk::guid::GUID& parentUniqueId() const {
        return header_.parent_unique_id;
    }

    uint32_t parentTimestamp() const {
        return footer_.timestamp;
    }

    const Footer& footer() const {
        return footer_;
    }

    uint32_t maxBatTableEntries() const {
        return header_.max_table_entries;
    }

    uint64_t batTableOffset() const {
        return header_.table_offset;
    }

    const BatEntry* batTable() const {
        return bat_entries_;
    }

    const std::string& parentAbsolutePath() const {
        return parent_absolute_path_;
    }

    const std::string& parentRelativePath() const {
        return parent_relative_path_;
    }

    int readBatEntryBitmap(uint64_t sector_num, BatEntry* bentry, uint8_t* buf);

    void show() const;
    
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
        VpcDiskType disk_type, 
        const std::string& parent_absolute_path = std::string(""), 
        const std::string& parent_relative_path = std::string(""));

    static uint32_t calcTimestamp();
    static uint32_t calcChecksum(const void* data, size_t len);
    static void     calcDiskGeometry(uint64_t total_sectors, Footer::DiskGeometry* dg);
    static void     footerByteOrderSwap(Footer* f);    
    static void     headerByteOrderSwap(Header* h);

    static void footerIn(Footer* f) {
        return footerByteOrderSwap(f);
    }
    static void headerIn(Header* h) {
        return headerByteOrderSwap(h);
    }
    static void footerOut(Footer* f) {
        return footerByteOrderSwap(f);
    } 
    static void headerOut(Header* h) {
        return headerByteOrderSwap(h);
    }

    int buildParentList(); 
    void blockTranslate(uint64_t sector_num, uint32_t nb_sectors, SectorInfo* si); 
    int  allocateNewBlock(uint64_t* new_offset);
    int  readRecursion(int32_t parent_index, uint64_t sector_num, uint32_t nb_sectors, uint8_t* buf);

    static int  readBatTable(int fd, uint64_t offset, uint8_t* bt_buf, size_t len);
    static int  writeBatTable(int fd, uint64_t offset, const uint8_t* bt_buf, size_t len);
    static int  readBitmap(int fd, uint64_t offset, uint8_t* bm_buf, size_t len);
    static int  writeBitmap(int fd, uint64_t offset, const uint8_t* bm_buf, size_t len);
    static int  readPayloadData(int fd, uint64_t offset, uint8_t* pld_buf, size_t len);
    static int  writePayloadData(int fd, uint64_t offset, const uint8_t* pld_buf, size_t len);
    static int  readFooter(int fd, uint64_t offset, uint8_t* f_buf);
    static int  writeFooter(int fd, uint64_t offset, const uint8_t* f_buf);

    std::string file_;
    int fd_;

    Footer footer_;
    Header header_;

    std::vector<uint8_t> bat_buf_;
    BatEntry* bat_entries_;

    uint32_t sectors_per_block_;
    // rewrite file end footer
    bool rewriter_footer_;

    std::string parent_absolute_path_;
    std::string parent_relative_path_;

    std::vector<std::unique_ptr<Vpc>> parents_;
};

}
#endif