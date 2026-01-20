
#include "utils.h"
#include "header.h"
#include "metadata.h"
#include "vhdx.h"

#include <cinttypes>
#include <cstdio>
#include <unistd.h>

void usage(const char* argv0) {
    printf("usage: %s /path/to/vhdx_file\n", argv0);
    printf("usage: %s -c [2|3] -s x[M|G|T] /path/to/vhdx_file\n", argv0);
    printf("usage: %s -c 4 -p /path/to/parent_vhdx_file /path/to/vhdx_file\n", argv0);
    //printf("usage: %s -c 4 -p /path/to/parent_vhdx_file -a 'parent_absolute_path' -e 'parent_relative_path' /path/to/vhdx_file\n", argv0);
    printf("usage: %s -m -a 'parent_absolute_path' -e 'parent_relative_path' /path/to/vhdx_file\n", argv0);
    printf("usage: %s -r sector_num[:sectors(default:1)] /path/to/vhdx_file\n", argv0);    
    printf("usage: %s -b sector_num /path/to/vhdx_file (read bat table per one chunk)\n", argv0);
}

int main(int argc, char* argv[]) {
    int disk_type = -1;
    bool modify_parent_locator = false;
    std::string file, parent_file;
    std::string parent_absolute_path, parent_relative_path;
    std::string disk_size;
    bool read_sectors = false;
    bool read_bat = false;    
    uint64_t sector_num = 0UL;
    uint32_t nb_sectors = 1;    
    int c;
    char unit;

    while ((c = getopt(argc, argv, "c:p:s:hma:e:r:b:")) != -1) {
        switch (c) {
        case 'c':
            disk_type = atoi(optarg);
            if (disk_type != 2 && disk_type != 3 && disk_type != 4) {
               usage(argv[0]);
               return 1;
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
        case 'r':
            {
                std::string rp(optarg);
                std::size_t pos = rp.find(':');
                if (pos == std::string::npos) {
                    sector_num = libvdk::convert::atoui64(rp.c_str());
                } else {
                    sector_num = libvdk::convert::atoui64(rp.substr(0, pos).c_str());
                    nb_sectors = libvdk::convert::atoui(rp.substr(pos+1).c_str());
                }
                read_sectors = true;
            }
            break;
        case 'b':
            sector_num = libvdk::convert::atoui64(optarg);
            read_bat = true;
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
            return vhdx::Vhdx::createFixed(file, size);
        } else if (disk_type == 3) {
            return vhdx::Vhdx::createDynamic(file, size);
        } else if (disk_type == 4) {
            return vhdx::Vhdx::createDifferencing(file, parent_file, parent_absolute_path, parent_relative_path);
        }        
    } else if (modify_parent_locator) {
        if (parent_absolute_path.empty() && parent_relative_path.empty()) {
            usage(argv[0]);
            return -1;
        }

        vhdx::Vhdx vhdx(file, false);
        if (vhdx.parse()) {
            return -1;
        }

        if (vhdx.diskType() != vhdx::metadata::VirtualDiskType::kDifferencing) {
            printf("file: %s type is not differencing\n", file.c_str());
            return -1;
        }

        return vhdx.modifyParentLocator(parent_absolute_path, parent_relative_path);
    } else if (read_sectors) {
        vhdx::Vhdx vhdx(file);
        if (vhdx.parse()) {
            return -1;
        }

        uint64_t max_sector_num = vhdx.diskSize() >> vhdx.logicalSectorSizeBits();
        if (sector_num >= max_sector_num) {
            printf("file: %s, requested #sector: %" PRIu64 " exceeds max #sector: %" PRIu64 "\n",
                file.c_str(), sector_num, max_sector_num);
            return -1;
        }

        //if (read_bat) {
            
        //}

        std::vector<uint8_t> buf(nb_sectors << vhdx.logicalSectorSizeBits(), 0);
        int ret = vhdx.read(sector_num, nb_sectors, buf.data());
        if (ret) {
            return -1;
        }

        uint32_t line = 0;
        char ascii_buf[16] = {'\0'};
        int ascii_index = 0;
        for (size_t i=0; i<buf.size(); ++i) {
            if (i % 16 == 0) {
                printf("%08X: ", line);
                line += 16;                
            }

            printf("%02X ", buf[i]);
            if (isprint(buf[i])) {
                ascii_buf[ascii_index++] = buf[i];
            } else {
                ascii_buf[ascii_index++] = '.';
            }

            if ((i+1) % 16 == 0) {
                for (int j = 0; j<ascii_index; ++j) {
                    printf("%c", ascii_buf[j]);
                }
                printf("\n");

                memset(ascii_buf, 0, sizeof(ascii_buf));
                ascii_index = 0;
            }
        }
    } else if (read_bat) {
        vhdx::Vhdx vhdx(file);
        if (vhdx.parse()) {
            return -1;
        }

        uint32_t bat_index = sector_num >> vhdx.sectorsPerBlockBits();
        bat_index += bat_index >> vhdx.chunkRatioBits();

        vhdx::bat::PayloadBatEntryStatus pstatus;
        uint64_t poffset;
        vhdx::bat::BatEntry pe = vhdx.bat()[bat_index];
        vhdx::bat::payloadBatStatusOffset(pe, &pstatus, &poffset);
        printf("#sector: %" PRIu64 ", payload bat index: %u, raw value: 0x%016lX\n", sector_num, bat_index, pe);
        printf("status: %s, offset: 0x%016lX\n\n", vhdx::Vhdx::payloadStatusToString(pstatus), poffset);

        if (vhdx.diskType() == vhdx::metadata::VirtualDiskType::kDifferencing) {
            uint32_t bat_idx_in_chunk = bat_index >> vhdx.chunkRatioBits();
            uint32_t bbat_index = ((bat_idx_in_chunk + 1) << vhdx.chunkRatioBits()) + bat_idx_in_chunk;             

            vhdx::bat::BitmapBatEntryStatus bstatus;
            uint64_t boffset;
            vhdx::bat::BatEntry be = vhdx.bat()[bbat_index];
            vhdx::bat::bitmapBatStatusOffset(be, &bstatus, &boffset);

            printf("#sector: %" PRIu64 ", bitmap bat index: %u, raw value: 0x%016lX\n", sector_num, bbat_index, be);
            printf("status: %s, offset: 0x%016lX\n\n", vhdx::Vhdx::bitmapStatusToString(bstatus), boffset);

            uint32_t line = 0;
            const vhdx::bat::BatEntry* p = vhdx.bat();
            //printf("total bat count: %u\n", vhdx.totalBatCount());
            uint32_t bat_idx_begin = bat_idx_in_chunk * vhdx.chunkRatio() + bat_idx_in_chunk;
            printf("bat index: %u, chunk bat index begin: %u\n", bat_index, bat_idx_begin);
            for (uint32_t i=0; i<=vhdx.chunkRatio(); ++i) {
                if (i % 4 == 0) {
                    printf("%08X: ", i);
                    line += 32;                
                }

                
                //uint32_t bat_idx_in_chunk = i >> vhdx.chunkRatioBits();
                //uint32_t bitmap_idx = ((bat_idx_in_chunk + 1) << vhdx.chunkRatioBits()) + bat_idx_in_chunk;
                //printf("%016lx [%u:%u]", p[i], i, bitmap_idx);
                printf("%016lx ", p[bat_idx_begin++]);
                // if (i != 0 && i == (bitmap_idx - vhdx.chunkRatio() - 1)) {
                //     printf("* ");
                // } else {
                //     printf("  ");
                // }

                if ((i+1) % 4 == 0) {
                    printf("\n");
                }            

                // if ((i+1) % vhdx.chunkRatio() == 0) {
                //     printf("\n");
                // }
            }
        }        
        printf("\n");        
    } else {
        // {
        //     std::vector<char> vec_buf;            
        // }
        vhdx::Vhdx vhdx(file);
        if (vhdx.parse()) {
            return -1;
        }

        vhdx.showHeaderSection();
        vhdx.showMetadataSection();  

        if (vhdx.buildParentList() == 0) {
            vhdx.showParentInfo();
        }
        // uint32_t bat_count = vhdx.totalBatCount();
        // const vhdx::bat::BatEntry* bat_array = vhdx.bat();
        // for (uint32_t i=0; i<bat_count; ++i) {
        //     printf("%02d: 0x%016lx\n", i, bat_array[i]);
        // }        
    }       

    return 0;   
}