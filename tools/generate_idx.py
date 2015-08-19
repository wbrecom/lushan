import struct
import sys

if len(sys.argv) < 3:
    print "usage:%s dat_file idx_file" % sys.argv[0]
    sys.exit(1)

dat_fp = open(sys.argv[1])
idx_fp = open(sys.argv[2], 'wb')

off = dat_fp.tell()
line = dat_fp.readline()
while line != '':
    pos = line.find(':')
    if pos > 0:
        id = int(line[0:pos])
        off += pos + 1
        data_idx = struct.pack('QQ', id, ((len(line) - pos - 1) << 40) | off)
        idx_fp.write(data_idx)
    off = dat_fp.tell()
    line = dat_fp.readline()

dat_fp.close()
idx_fp.close()
