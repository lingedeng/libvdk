#ifndef LIBVDK_VHD_METADATA_H_
#define LIBVDK_VHD_METADATA_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "utils.h"
#include "common.h"

namespace vhdx {
namespace metadata {

#pragma pack(push, 1)

struct TableHeader {
    char signature[8];          // MUST be 0x617461646174656D ("metadata" as ASCII)
    uint16_t reserved;          // MUST be set to 0.
    uint16_t entry_count;       // Specifies the number of entries in the table. This value must be less than or equal to 2,047
    char reserved2[20];         // MUST be set to 0
};

// 定义所有的状态标志和位掩码
enum class TableEntryFlags : uint32_t {
    // 1. 单个标志位 (占 1 位)
    // 0000 0001
    kIsUser = 1 << 0, 
    // 0000 0010
    kIsVirtualDisk = 1 << 1, 
    // 0000 0100
    kIsRequired = 1 << 2,
};

/*
Known Items	                GUID	                            IsUser	IsVirtualDisk	IsRequired
File Parameters	        CAA16737-FA36-4D43-B3B6-33F0AA44E76B	False	False	        True
Virtual Disk Size	    2FA54224-CD1B-4876-B211-5DBED83BF4B8	False	True	        True
Virtual Disk ID	        BECA12AB-B2E6-4523-93EF-C309E000C746	False	True	        True
Logical Sector Size	    8141BF1D-A96F-4709-BA47-F233A8FAAB5F	False	True	        True
Physical Sector Size	CDA348C7-445D-4471-9CC9-E9885251C556	False	True	        True
Parent Locator	        A8D35F2D-B30B-454D-ABF7-D3D84834AB0C	False	False	        True
*/
// TableHeader + TableEntry = 64K
struct TableEntry {
    libvdk::guid::GUID   item_id;           // The ItemId and IsUser value pair for an entry MUST be unique within the table
    uint32_t offset;            // Specifies the byte offset of the metadata item in bytes from TableHeader begin
    uint32_t length;            // Specifies the length of the metadata item in bytes, include all data length
    TableEntryFlags flags;             // 0bit - A(IsUser), 1bit - B(IsVirtualDisk), 2bit - C(IsRequired)
    uint32_t reserved;                 // D - Reserved2 (2 bits): MUST be set to 0
};

struct TableHeaderEntry {
    TableHeader table_header_;
    //std::unique_ptr<TableEntry> up_table_entries_;
    TableEntry  well_known_table_entries_[6];
};

enum class FileParametersFlags : uint32_t {
    // 1. 单个标志位 (占 1 位)
    // 0000 0001
    // This field is intended to be used to create a fixed VHDX file that is fully provisioned
    kLeaveBlockAllocated = 1 << 0, 
    // 0000 0010
    kHasParent = 1 << 1,
};

struct FileParameters {
    uint32_t block_size_in_bytes;        // Specifies the size of each payload block in bytes. default: 0x02000000 = 32M
    FileParametersFlags flags;  // A - LeaveBlockAllocated (0 bit), B - HasParent (1 bit)
};

struct VirtualDiskSize {
    uint64_t size_in_bytes;              // the virtual disk size, in bytes
};

struct VirtualDiskId {
    libvdk::guid::GUID guid;                // A GUID that specifies the identification of the disk
};

struct LogicalSectorSize {
    uint32_t size_in_bytes;              // the virtual disk's sector size(in bytes). This value MUST be set to 512(default) or 4,096
};

struct PhysicalSectorSize {
    uint32_t size_in_bytes;              // the virtual disk's physical sector size(in bytes). This value MUST be set to 512 or 4,096(default)
};

struct ParentLocatorHeader {
    libvdk::guid::GUID locator_type_guid;        // the type of the parent virtual disk.
    uint16_t reserved;
    uint16_t key_value_count;   // the number of key-value pairs defined for this parent locator
};

/*
 * possible key-value pair entries
Entry	                Type	Example
parent_linkage	        GUID	{83ed0ec3-24c8-49a6-a959-5e4bf1288bfb}
parent_linkage2	        GUID	{83ed0ec3-24c8-49a6-a959-5e4bf1288bfb}
relative_path	        Path	..\..\path2\sub3\parent.vhdx
volume_path	            Path	\\?\Volume{26A21BDA-A627-11D7-9931-806E6F6E6963}\path2\sub3\parent.vhdx
absolute_win32_path	    Path	\\?\d:\path2\sub3\parent.vhdx
*/
// The key and value strings are to be UNICODE strings with UTF-16 little-endian encoding. 
// There must be no internal NUL characters, and the Length field must not include a trailing NUL character
struct ParentLocatorEntry {
    uint32_t key_offset;    // 相对于ParentLocatorHeader开始位置的偏移
    uint32_t value_offset;  // 相对于ParentLocatorHeader开始位置的偏移
    uint16_t key_length;
    uint16_t value_length;
};

struct ParentLocator {
    ParentLocatorHeader header;
    ParentLocatorEntry  entries[5];
};

#pragma pack(pop)

class MetadataSection {
public:
    MetadataSection();
    ~MetadataSection() = default;

    void initContent(vhdx::metadata::VirtualDiskType type, uint64_t vdk_size_in_bytes, 
        uint32_t block_size, uint32_t logical_sector_size, uint32_t physical_sector_size);
    // When a differencing VHDX file is created
    // linkage value MUST populate the parent's DataWriteGuid field 
    int initParentLocatorContent(const std::string& file, const std::string& parent_file, 
        const std::string& linkage, const std::string& parent_absolute_path, const std::string& parent_relative_path);
    int writeContent(int fd);
    int parseContent(int fd, uint64_t offset);

    int modifyParentLocator(int fd, uint64_t metadata_offset, 
        const std::string& parent_absolute_path, const std::string& parent_relative_path);

    // const FileParameters& fileParameters() const {
    //     return file_parameters_;
    // }

    // const VirtualDiskSize& virtualDiskSize() const {
    //     return virtual_disk_size_;
    // }

    const libvdk::guid::GUID& virtualDiskGuid() const {
        return virtual_size_guid_;
    }

    // const LogicalSectorSize& logicalSectorSize() const {
    //     return logical_sector_size_;
    // }

    // const PhysicalSectorSize& physicalSectorSize() const {
    //     return physical_sector_size_;
    // }    

    uint32_t blockSizeInMb() const {
        return (file_parameters_.block_size_in_bytes >> 20);
    }

    uint32_t blockSize() const {
        return file_parameters_.block_size_in_bytes;
    }

    uint64_t diskSize() const {
        return virtual_disk_size_.size_in_bytes;
    }

    uint32_t logicalSectorSize() const {
        return logical_sector_size_.size_in_bytes;
    }

    uint32_t physicalSectorSize() const {
        return physical_sector_size_.size_in_bytes;
    }

    uint32_t chunkRatio() const {
        return chunk_ratio_;
    }    

    uint32_t dataBlockCount() const {
        return data_block_count_;
    }

    uint32_t bitmapBlockCount() const {
        return bitmap_block_count_;
    } 

    uint64_t totalBatCount() const {
        return total_bat_count_;
    }

    uint64_t totalBatSizeInBytes() const {
        return total_bat_count_*8;
    }

    uint64_t batOccupySizeInBytes() const {
        return libvdk::convert::roundUp(totalBatSizeInBytes(), libvdk::kMiB);
    }

    uint32_t batOccupyMbCount() const {
        return (batOccupySizeInBytes() >> libvdk::kMibShift);
    }

    uint32_t sectorsPerBlocks() const {
        return sectors_per_block_;
    }

    uint32_t blockSizeBits() const {
        return block_size_bits_;
    }

    uint32_t logicalSectorSizeBits() const {
        return logical_sector_size_bits_;
    }

    uint32_t chunkRatioBits() const {
        return chunk_ratio_bits_;
    }

    uint32_t sectorsPerBlockBits() const {
        return sectors_per_block_bits_;
    }

    vhdx::metadata::VirtualDiskType diskType() const {
        if (file_parameters_.flags == FileParametersFlags::kLeaveBlockAllocated) {
            return vhdx::metadata::VirtualDiskType::kFixed;
        } else if (file_parameters_.flags == FileParametersFlags::kHasParent) {
            return vhdx::metadata::VirtualDiskType::kDifferencing;
        } else {
            return vhdx::metadata::VirtualDiskType::kDynamic;
        }
    }

    const std::string& parentLinkage() const {
        return parent_linkage_;
    }
    std::string parentLinkageForCompare() {
        return parent_linkage_.substr(1, parent_linkage_.size()-2);
    }
    const std::string& parentLinkage2() const {
        return parent_linkage2_;
    }
    const std::string parentRelativePath() const {
        return parent_relative_path_;
    }
    const std::string parentVolumePath() const {
        return parent_volume_path_;
    }    
    const std::string parentAbsoluteWin32Path() const {
        return parent_absolute_win32_path_;
    }

    // void setParentRelativePath(const std::string& path) {
    //     parent_relative_path_ = path;
    // }

    // void setParentAbsoluteWin32Path(const std::string& path) {
    //     parent_absolute_win32_path_ = path;
    // }

    void show() const;
private:
    struct ParentLocatorWithKvData {
        ParentLocator locator;
        std::vector<char> data;
        //std::string data;
    };

    void calcBatInfo();
    void initParentLocatorData(size_t parent_locator_table_entry_index);    
    void initParentLocatorEntryKeyValue(const wchar_t* key, 
            size_t *ple_index, size_t *kv_offset, std::string* buf, size_t* buf_len);
    void initParentLocatorHeader();
    int  writeParentLocatorContent(int fd);

    TableHeaderEntry table_header_entries_;

    FileParameters file_parameters_;
    VirtualDiskSize virtual_disk_size_;
    libvdk::guid::GUID virtual_size_guid_;
    LogicalSectorSize logical_sector_size_;
    PhysicalSectorSize physical_sector_size_;

    //ParentLocator parent_locator_;
    ParentLocatorWithKvData parent_locator_with_data_;

    std::string parent_linkage_;
    std::string parent_linkage2_;
    std::string parent_relative_path_;
    std::string parent_volume_path_;
    std::string parent_absolute_win32_path_;       

    /*
     64T, 1Mb block size
     chunk_ratio_ = 4096
     data_block_count_ = 67108864
     bitmap_block_count_ = 16384
     total_bat_count = 67125247(dyn) | 67125248(diff)
     */

    /*
     chunk_ratio_ = (1 << 23) * 512 / file_parameters_.block_size_in_bytes; 
     */
    uint32_t chunk_ratio_;    
    /*
     data_block_count_ = ceil(virtaul_disk_size_ / file_parameters_.block_size_in_bytes)
    */
    uint32_t data_block_count_;
    /*
    bitmap_block_count_ = ceil(data_block_count_ / chunk_ratio_);    
    */
    uint32_t bitmap_block_count_;

    /*
     For dynamic:
     total_bat_count_ = data_block_count_ + floor((data_block_count - 1) / chunk_ratio_);
     For differencing:
     total_bat_count_ = bitmap_block_count_ * (chunk_ratio_ + 1);
     */
    uint32_t total_bat_count_;

    uint32_t sectors_per_block_;

    uint32_t block_size_bits_;
    uint32_t logical_sector_size_bits_;
    uint32_t chunk_ratio_bits_;
    uint32_t sectors_per_block_bits_;

};

} // namespace metadata
} // namespace vhdx

#endif