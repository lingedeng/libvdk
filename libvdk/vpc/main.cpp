
#include "utils.h"

#include <cinttypes>
#include <cstdio>
#include <unistd.h>

#include "vpc.h"

void printContent(const uint8_t *buf, size_t len, bool show_ascii);

void usage(const char* argv0) {
    printf("usage: %s /path/to/vhd_file\n", argv0);
    printf("usage: %s -c [2|3] -s x[M|G|T] /path/to/vhd_file\n", argv0);
    printf("usage: %s -c 4 -p /path/to/parent_vhdx_file /path/to/vhd_file\n", argv0);
    //printf("usage: %s -c 4 -p /path/to/parent_vhdx_file -a 'parent_absolute_path' -e 'parent_relative_path' /path/to/vhdx_file\n", argv0);
    printf("usage: %s -m -a 'parent_absolute_path' -e 'parent_relative_path' /path/to/vhd_file\n", argv0);
    printf("usage: %s -r sector_num[:sectors(default:1)] /path/to/vhd_file\n", argv0);
    printf("usage: %s -w sector_num[:sectors(default:1)] /path/to/vhd_file (for test)\n", argv0);
    printf("usage: %s -b sector_num /path/to/vhd_file\n", argv0);    
    printf("usage: %s -c 0 /path/to/vhd_file (empty dynamic or differencing)\n", argv0); 
}

int main(int argc, char* argv[]) {
    int disk_type = -1;
    bool modify_parent_locator = false;
    std::string file, parent_file;
    std::string parent_absolute_path, parent_relative_path;
    std::string disk_size;
    bool read_sectors = false;
    bool write_sectors = false;
    bool read_bat_bitmap = false;
    bool empty_disk = false;
    // uint32_t bat_index = 0;
    // uint32_t bat_count = 1;
    uint64_t sector_num = 0UL;
    uint32_t nb_sectors = 1;    
    int c;
    char unit;

    while ((c = getopt(argc, argv, "c:p:s:hma:e:r:w:b:")) != -1) {
        switch (c) {
        case 'c':
            disk_type = atoi(optarg);            
            if (disk_type != 0 && disk_type != 2 && disk_type != 3 && disk_type != 4) {
               usage(argv[0]);
               return 1;
            }
            if (disk_type == 0) {
                empty_disk = true;
                disk_type = -1;
            } 
            break;
        case 'p':
            parent_file = optarg;
            break;
        case 's':            
            disk_size =  optarg;
            unit = disk_size[disk_size.size()-1];
            if (unit != 'M' && unit != 'G' && unit != 'T') {
               usage(argv[0]);
               return 1;
            }
            break;
        case 'a':
            parent_absolute_path = optarg;
            break;
        case 'e':
            parent_relative_path = optarg;
            break;
        case 'm':
            modify_parent_locator = true;
            break;
        case 'b':
            {
                // std::string rp(optarg);
                // std::size_t pos = rp.find(':');
                // if (pos == std::string::npos) {
                //     bat_index = libvdk::convert::atoui(rp.c_str());
                // } else {
                //     bat_index = libvdk::convert::atoui(rp.substr(0, pos).c_str());
                //     bat_count = libvdk::convert::atoui(rp.substr(pos+1).c_str());
                // }
                sector_num = libvdk::convert::atoui64(optarg);
                read_bat_bitmap = true;
            }
            break;
        case 'r':
        case 'w':
            {
                std::string rp(optarg);
                std::size_t pos = rp.find(':');
                if (pos == std::string::npos) {
                    sector_num = libvdk::convert::atoui64(rp.c_str());
                } else {
                    sector_num = libvdk::convert::atoui64(rp.substr(0, pos).c_str());
                    nb_sectors = libvdk::convert::atoui(rp.substr(pos+1).c_str());
                }
                if (c == 'r') {
                    read_sectors = true;
                } else {
                    write_sectors = true;
                }
            }
            break;
        case 'h':
            usage(argv[0]);
            return 0;
        case '?':
            if (optopt == 'c' || optopt == 'p' || optopt == 's')
                fprintf(stderr, "Option -%c requires an argument.\n", optopt);            
            else if (isprint (optopt))
                fprintf (stderr, "Unknown option `-%c'.\n", optopt);
            else
                fprintf (stderr, "Unknown option character `\\x%x'.\n", optopt);
            return 1;        
        }
    }

    // printf("optind: %d\n", optind);
    // printf("disk type: %d, disk size: %s, parent file: %s\n", 
    //     disk_type, disk_size.c_str(), parent_file.c_str());

    if (optind < argc) {
        file = argv[optind];
    } else {
        usage(argv[0]);        
        return -1;
    }

    if (disk_type != -1) {
        if (disk_type == 4 && parent_file.empty()) {            
            return -1;
        }

        uint64_t size = 0;
        if (disk_type == 2 || disk_type == 3) {
            char unit = disk_size[disk_size.size()-1]; 
            uint32_t value = atoi(disk_size.substr(0, disk_size.size()-1).c_str());         
            if (unit == 'M') {
                size = value * libvdk::kMiB;
            } else if (unit == 'G') {
                size = value * libvdk::kGiB;
            } else if (unit == 'T') {
                size = value * libvdk::kTiB;
            }

            if (size == 0 || size > 64 * libvdk::kTiB) {
                printf("disk size must > 0 and the max is 64T\n");
                return -1;
            }
        }               
        
        if (disk_type == 2) {
            return vpc::Vpc::createFixed(file, size);
        } else if (disk_type == 3) {
            return vpc::Vpc::createDynamic(file, size);
        } else if (disk_type == 4) {
            return vpc::Vpc::createDifferencing(file, parent_file, parent_absolute_path, parent_relative_path);
        }        
    } else if (modify_parent_locator) {
        if (parent_absolute_path.empty() && parent_relative_path.empty()) {
            usage(argv[0]);
            return -1;
        }

        vpc::Vpc v(file, false);
        if (v.parse(false)) {
            return -1;
        }

        if (v.diskType() != vpc::VpcDiskType::kDifferencing) {
            printf("file: %s type is not differencing\n", file.c_str());
            return -1;
        }

        return v.modifyParentLocator(parent_absolute_path, parent_relative_path);
    } else if (read_sectors) {
        vpc::Vpc v(file);
        if (v.parse()) {
            return -1;
        }

        uint64_t max_sector_num = v.diskSize() >> vpc::kSectorBytesShift;
        if (sector_num >= max_sector_num) {
            printf("file: %s, requested #sector: %" PRIu64 " exceeds max #sector: %" PRIu64 "\n",
                file.c_str(), sector_num, max_sector_num);
            return -1;
        }

        uint32_t bat_idx = sector_num / (vpc::kBlockSize >> vpc::kSectorBytesShift);
        vpc::BatEntry bentry = v.batTable()[bat_idx];
        printf("sector num: %" PRIu64 " at bat table[%u]: 0x%08X\n", sector_num, bat_idx, bentry);

        std::vector<uint8_t> buf(nb_sectors << vpc::kSectorBytesShift, 0);
        int ret = v.read(sector_num, nb_sectors, buf.data());
        if (ret) {
            return -1;
        }

        printContent(buf.data(), buf.size(), true);
    } else if (read_bat_bitmap) {
        vpc::Vpc v(file);
        if (v.parse()) {
            return -1;
        }

        uint64_t max_sector_num = v.diskSize() >> vpc::kSectorBytesShift;
        if (sector_num >= max_sector_num) {            
            printf("file: %s, requested #sector: %" PRIu64 " exceeds max #sector: %" PRIu64 "\n",
                file.c_str(), sector_num, max_sector_num);
            return -1;        
        }
        uint8_t bitmap_buf[vpc::kBitmapSize];
        vpc::BatEntry bentry;

        int ret = v.readBatEntryBitmap(sector_num, &bentry, bitmap_buf);
        if (ret) {
            return ret;
        }

        printf("sector num: %" PRIu64 ", bat entry: 0x%08X\n", sector_num, bentry);
        if (bentry != vpc::kBatEntryUnused) {
            printf("the sector belongs block bitmap:\n");
            printContent(bitmap_buf, vpc::kBitmapSize, false);
        } else {
            printf("the sector belongs block is not allocated\n");
        }
    } else if (write_sectors) {
        vpc::Vpc v(file, false);
        if (v.parse()) {
            return -1;
        }

        std::vector<uint8_t> buf(vpc::kSectorSize*nb_sectors, 0);
        for (size_t i=0; i<buf.size(); ++i) {
            buf[i] = i;
        }

        int ret = v.write(sector_num, nb_sectors, buf.data());
        if (ret) {
            return -1;
        }
    } else if (empty_disk) {
        return vpc::Vpc::emptyDisk(file);
    } else {
        vpc::Vpc v(file);
        if (v.parse()) {
            return -1;
        }

        v.show();        
    }          

    return 0;
}

void printContent(const uint8_t *buf, size_t len, bool show_ascii) {
    uint32_t line = 0;
    char ascii_buf[16] = {'\0'};
    int ascii_index = 0;
    for (size_t i=0; i<len; ++i) {
        if (i % 16 == 0) {
            printf("%08X: ", line);
            line += 16;                
        }

        printf("%02X ", buf[i]);
        if (show_ascii) {
            if (isprint(buf[i])) {
                ascii_buf[ascii_index++] = buf[i];
            } else {
                ascii_buf[ascii_index++] = '.';
            }
        }

        if ((i+1) % 16 == 0) {
            if (show_ascii) {
                for (int j = 0; j<ascii_index; ++j) {
                    printf("%c", ascii_buf[j]);
                }

                memset(ascii_buf, 0, sizeof(ascii_buf));
                ascii_index = 0;
            }
            printf("\n");            
        }
    }
}