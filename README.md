Microsoft vhd and vhdx file operation: create, read, write
```
// 查看文件基本信息
usage: ./vhdx /path/to/vhdx_file

// 创建文件: 2 - Fixed, 3 - Dynamic
usage: ./vhdx -c (2|3) -s x(M|G|T) /path/to/vhdx_file

// 创建差异文件
usage: ./vhdx -c 4 -p /path/to/parent_vhdx_file /path/to/vhdx_file

// 修改差异文件的父路径
usage: ./vhdx -m [-a 'parent_absolute_path'] [-e 'parent_relative_path'] /path/to/vhdx_file

// 查看扇区内容
usage: ./vhdx -r sector_num[:sectors(default:1)] /path/to/vhdx_file

// 查看扇区所在BAT表信息
usage: ./vhdx -b sector_num /path/to/vhdx_file (read bat table per one chunk)

// 查看日志
usage: ./vhdx -l /path/to/vhdx_file (show log)
```
