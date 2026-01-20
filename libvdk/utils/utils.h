#ifndef LIBVDK_UTILS_UTILS_H_
#define LIBVDK_UTILS_UTILS_H_

#include <uuid/uuid.h>

#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <string>
#include <cstdio>
#include <memory>

#if defined(__linux__)
#include <endian.h>
#include <byteswap.h>	//bswap_[16|32|64]
#elif defined(__NetBSD__)
#include <sys/endian.h>
#include <sys/bswap.h>	//bswap[16|32|64]
#endif

namespace libvdk {
    const uint32_t kKibShift = 10;
    const uint32_t kMibShift = 20;
    const uint32_t kGibShift = 30;
    const uint32_t kTibShift = 40;

    const uint64_t kKiB = (1UL << kKibShift);
    const uint64_t kMiB = (1UL << kMibShift);
    const uint64_t kGiB = (1UL << kGibShift);
    const uint64_t kTiB = (1ULL << kTibShift);

namespace file {
    int create_file(const std::string& file_path);
    int open_file_ro(const std::string& file_path);
    int open_file_rw(const std::string& file_path);
    inline int close_file(int fd) {
        return ::close(fd);
    }
    inline int delete_file(const std::string& file_path) {
        return ::unlink(file_path.c_str());
    }
    
    // 设置文件偏移
    //int seek_file_li(int fd, LARGE_INTEGER offset, int whence);
    int seek_file(int fd, off64_t offset, int whence);

    int read_file(int fd, void* buf, size_t size);
    int write_file(int fd, const void* buf, size_t size);

    //int get_file_sizes_li(int fd, LARGE_INTEGER* pos);
    int get_file_sizes(int fd, int64_t* pos);

    //int get_file_pos_li(int fd, LARGE_INTEGER* pos);
    int get_file_pos(int fd, off64_t* pos);

    int truncate_file(int fd, off64_t offset); 
    
    std::string absolute_path(const std::string& file, int* err);
    std::string relative_path_to(const std::string& file, const std::string& another_file, int* err);

    // if file exist, zero is returned
    inline int exist_file(const std::string& file_path) {
        return ::access(file_path.c_str(), F_OK);
    }
} // namespace file

namespace guid {
    const int kMaxUUID = 40;

    typedef struct uuid_s {
        union
        {
            struct
            {
                uint32_t        Data1;
                uint16_t        Data2;
                uint16_t        Data3;                
                unsigned char   Data4[8];
            };
            uuid_t	uuid;
        };
        
        bool operator==(const uuid_s& rhs) const {
            if (Data1 != rhs.Data1) return false;
            if (Data2 != rhs.Data2) return false;
            if (Data3 != rhs.Data3) return false;            
            if (memcmp(Data4, rhs.Data4, sizeof(Data4)) != 0) return false;
            
            return true;
        }

        bool operator!=(const uuid_s& rhs) const {
            return !(*this == rhs);
        }
    } UUID;

    using GUID = UUID;

    inline void generate(GUID *out) {
        uuid_generate(out->uuid);
    }

    inline std::string toString(const GUID *in, bool uppercase = true) {
        char buf[kMaxUUID] = {'\0'};
        if (uppercase) {
            uuid_unparse_upper(in->uuid, buf);
        } else {
            uuid_unparse_lower(in->uuid, buf);
        }
        return buf;
    }

    const GUID kNullGuid = {0x00, 0x00, 0x00, {0x00}};

    std::string toWinString(const GUID *in, bool uppercase = true);
} // namespace guid

namespace byteorder {
    inline void swap16(uint16_t* value) {
#if BYTE_ORDER == LITTLE_ENDIAN
#if defined(__linux__)
			*value = bswap_16(*value);
#elif defined(__NetBSD__)
			*value = bswap16(*value);
#else		
			*value =  (*value << 8) | (*value >> 8);
#endif
#endif
		}

		inline void swap32(uint32_t* value) {
#if BYTE_ORDER == LITTLE_ENDIAN
#if defined(__linux__)
			*value = bswap_32(*value);
#elif defined(__NetBSD__)
			*value = bswap32(*value);
#else		
			*value = ((*value & 0x00ff00ff) << 8) | ((*value & 0xff00ff00) >> 8);
			*value =  (*value << 16) | (*value >> 16);
#endif
#endif			
		}

		inline void swap64(uint64_t* value) {		
#if BYTE_ORDER == LITTLE_ENDIAN
#if defined(__linux__)
			*value = bswap_64(*value);
#elif defined(__NetBSD__)
			*value = bswap64(*value);
#else		
			*value = ((*value & 0x00ff00ff00ff00ffull) << 8) | ((*value & 0xff00ff00ff00ff00ull) >> 8);
			*value = ((*value & 0x0000ffff0000ffffull) << 16) | ((*value & 0xffff0000ffff0000ull) >> 16);
			*value =  (*value << 32) | (*value >> 32);
#endif
#endif	
		}
} // namespace byteorder

namespace convert {
    // Round number down to multiple, Requires that size be a power of 2
    inline uint64_t roundDown(uint64_t bytes, uint32_t size) {
        return (bytes & static_cast<int64_t>(-(0 ? bytes : size)));
    }
    // round bytes up to multiple， Requires that size be a power of 2
    inline uint64_t roundUp(uint64_t bytes, uint32_t size) {
        return roundDown((bytes + size - 1), size);
    }
    // 向上取整
    inline uint64_t divRoundUp(uint64_t bytes, uint32_t size) {
        return (bytes + size - 1) / size;
    }

    inline int64_t     atoi64(const char* str) {
        return ::strtol(str, NULL, 10);
    }			
    inline uint64_t    atoui64(const char* str) {
        return ::strtoul(str, NULL, 10);
    }
	inline int32_t     atoi(const char* str) {
        return static_cast<int32_t>(::strtol(str, NULL, 10));
    }
	inline uint32_t    atoui(const char* str) {
        return static_cast<uint32_t>(::strtoul(str, NULL, 10));
    }

    inline uint32_t ctz32(uint32_t v) {
        if (v == 0) return 32;
        uint32_t count = 0;
        while ((v & 1) == 0) {
            ++count;
            v >>= 1;
        }
        return count;
    }

    inline uint32_t ctz64(uint64_t v) {
        if (v == 0) return 64;
        uint32_t count = 0;
        while ((v & 1) == 0) {
            ++count;
            v >>= 1;
        }
        return count;
    }
    
    std::string integerToString(int16_t);
    std::string integerToString(uint16_t);
    std::string integerToString(int32_t);
    std::string integerToString(uint32_t);
    std::string integerToString(int64_t);
    std::string integerToString(uint64_t);

    std::string wchar2Utf8(const wchar_t* unicode, bool unicode_le = true);

    /*
     example:
        Utf8ToUnicodeWrapper w("whoami");
        w.unicodeStr();
        w.unicodeLen();
    */
class Utf8ToUnicodeWrapper {
public:
    explicit Utf8ToUnicodeWrapper(bool unicode_le = true);
    explicit Utf8ToUnicodeWrapper(const char* str, bool unicode_le = true);
    explicit Utf8ToUnicodeWrapper(const std::string& str, bool unicode_le = true);        
    ~Utf8ToUnicodeWrapper();

    Utf8ToUnicodeWrapper(const Utf8ToUnicodeWrapper&) = delete;
    Utf8ToUnicodeWrapper& operator=(const Utf8ToUnicodeWrapper&) = delete;

    void convert(const char* str);
    void convert(const std::string& str);

    const wchar_t* unicodeStr() const {
        return reinterpret_cast<wchar_t*>(w_str_);
    }
    size_t unicodeLen() const {
        return len_;
    }

    const char* str() const {
        return w_str_;
    }
    size_t len() const {
        return len_*2;
    }
    
private:
    char*   w_str_;
    size_t  len_;   // unicode char length
    bool    unicode_le_;
};    
    
} // namespace convert

namespace encrypt {
    uint32_t crc32(const char* data, size_t len);
    
    // from leveldb/crc32c
    uint32_t Extend(uint32_t crc, const char* data, size_t len);
    inline uint32_t crc32c(const char* data, size_t len) {
        return Extend(0, data, len);
    }

    uint32_t checksum(const uint8_t* data, size_t len);
} // namespace encrypt

#ifdef CONSLOG
#undef CONSLOG
#endif

#define CONSLOG(format, ...) fprintf(stderr, "[%s - " __FILE__ ":%d]: " format "\n", __func__, __LINE__, ##__VA_ARGS__)

} // namespace libvdk

#endif