#!/bin/env python
# ddfs.py
#
# ddumpfs utility
# 

import sys, os, struct, base64, time, stat, mmap
import random, subprocess
import shutil
from optparse import OptionParser

try:
    import hashlib
except ImportError:
    hashlib=None
    import md5, sha

__version__='0.7.0'

NODE_BLOCK_SIZE=4096

def bitstoaddress(size):
    """return how many bits and octets are required to handle 'size' memory"""
    b=0
    x=1
    while x<size:
        x=x*2
        b+=1
    o=b/8
    if b%8:
        o+=1
    return b, o


def friendly_size(size):
    if not size:
        raise ValueError
    if size[-1] in 'kK':
        return int(size[:-1])*1024
    if size[-1] in 'mM':
        return int(size[:-1])*1024**2
    if size[-1] in 'gG':
        return int(size[:-1])*1024**3
    if size[-1] in 'tT':
        return int(size[:-1])*1024**4
    
    return int(size)


class Header:

    header_size=8
    header_fmt="!Q"
      
    def __init__(self, data=None):
        self.size=0
        self.new=True
        if data:
            self.load(data)
    
    def load(self, data):
        (self.size, )=struct.unpack(self.header_fmt, data)
        self.new=False
        
    def get(self):
        return struct.pack(self.header_fmt, self.size)

def hexdump(st):
    if isinstance(st, basestring):
        return ''.join(map(lambda c: '%02x' % ord(c), st))
    else: 
        return ''.join(map(lambda c: '%02x' % c, st)) 

class bit_array:
    
    def __init__(self, size, pattern=0x0, buffer=None):
        self.size=size
        self.isize=((size+31)/32)*4 # align on 4 bytes as in the C code
        if buffer:
            self.array=buffer
            self.ord=ord
            self.chr=chr
            self.empty='\0'
            self.full=chr(0xFF)
        else:
            self.array=bytearray(chr(pattern)*self.isize)
            self.ord=lambda x:x
            self.chr=lambda x:x
            self.empty=0
            self.full=0xFF
        self.index=0 # start search for first free block at 0 

    def set(self, addr, value=True):
        """return previous state"""
        byte_addr=addr/8
        bit_addr=7-addr%8
        old=self.ord(self.array[byte_addr]) & (1<<bit_addr)
        if value:
            self.array[byte_addr]=self.chr(self.ord(self.array[byte_addr]) | (1<<bit_addr))
        else:
            self.array[byte_addr]=self.chr((self.ord(self.array[byte_addr]) & ~(1<<bit_addr)) & 0xFF)
        return old

    def unset(self, addr):
        """return previous state"""
        return self.set(addr, False)

    def get(self, addr):
        byte_addr=addr/8
        bit_addr=7-addr%8
        return self.ord(self.array[byte_addr]) & (1<<bit_addr)

    def search_first_set(self, from_pos):
        i=from_pos/8
        off=from_pos%8
        while i*8<self.size:
            while (i*8<self.size and self.array[i]==self.empty):
                i+=1
                off=0
            
            if i*8>=self.size:
                return -1
            
            b=self.ord(self.array[i])<<off
            for j in range(off, 8):
                if b & 0x80:
                    addr=i*8+j;
                    if addr<self.size:
                        return addr
                    else:
                        return -1
                b=b<<1
            i+=1
            off=0
        return -1

    def search_first_unset(self, from_pos):
        i=from_pos/8
        off=from_pos%8
        while i*8<self.size:
            while (i*8<self.size and self.array[i]==self.full):
                i+=1
                off=0
            
            if i*8>=self.size:
                return -1
            
            b=self.ord(self.array[i])<<off
            for j in range(off, 8):
                if (b & 0x80)==0:
                    addr=i*8+j;
                    if addr<self.size:
                        return addr
                    else:
                        return -1
                b=b<<1
            i+=1
            off=0
        return -1
             
    def invers_into(self, ba):
        for i, b in enumerate(self.array):
            ba.array[i]=self.chr((~self.ord(b)) & 0xFF)

    def bwand(self, ba):
        """bitwise and"""
        for i, b in enumerate(ba.array):
            self.array[i]=self.chr(self.ord(self.array[i]) & b)

    def alloc(self):
        addr=self.search_first_unset(self.index)
        if addr==-1:
            addr=self.search_first_unset(1)
            
        if addr==-1:
            self.index=1
        else:
            if addr<self.size:
                self.index=addr+1
            else:
                self.index=1
            self.set(addr)
        return addr

    def count(self):
        s=u=0
        i=0
        n=8
        while i*8<self.size:
            b=self.ord(self.array[i])
            if (i+1)*8>self.size:
                n=8-(((i+1)*8)%self.size)
            if b==0:
                u+=n
            elif b==0xFF:
                s+=n
            else:
                for j in range(n):
                    if b & 0x80:
                        s+=1
                    else:
                        u+=1
                    b=b<<1
            i+=1
        assert s+u==self.size
        return s, u

    def count_diff(self, ba):
        d1=d2=0
        i=0
        n=8
        while i*8<self.size:
            a1=self.ord(self.array[i])
            a2=ba.ord(ba.array[i])
            
            if (i+1)*8>self.size:
                n=8-(((i+1)*8)%self.size)
                
            b=a1 & (~a2)
            if b==0:
                pass
            elif b==0xFF:
                d1+=n
            else:
                for j in range(n):
                    if b & 0x80:
                        d1+=1
                    b=b<<1
                    
            b=a2 & (~a1)
            if b==0:
                pass
            elif b==0xFF:
                d2+=n
            else:
                for j in range(n):
                    if b & 0x80:
                        d2+=1
                    b=b<<1
                    
            i+=1

        return d1, d2

        
    def save(self, offset, size):
        size=(size+31)/32*32
        off=offset/8
        return str(self.array[off:off+size/8])

    def load(self, buf, offset):
        off=offset/8
        for c in buf:
            self.array[off]=ord(c)
            off+=1
                        
    def __str__(self):
        st=''
        for c in self.array:
            b=bin(self.ord(c))[2:]
            b='0'*(8-len(b))+b
            st+=b
        return st[:self.size]
    

        
def test_bit_array():
    test_size=60 
    ba=bit_array(test_size, 0)
    bb=bit_array(test_size, 0)
    
    print ba
    ba.set(1)
    ba.set(58)
    ba.set(4)
    ba.set(55)
    print ba
    
    print "search first set from 0 : %d" % ba.search_first_set(0)
    print "search first set from 1 : %d" % ba.search_first_set(1)
    print "search first set from 2 : %d" % ba.search_first_set(2)
    print "search first set from 15 : %d" % ba.search_first_set(15)
    print "search first set from 55 : %d" % ba.search_first_set(55)
    print "search first set from 56 : %d" % ba.search_first_set(56)
    print "search first set from 59 : %d" % ba.search_first_set(59)
    
    ba.invers_into(bb)
    print bb
    print "search first unset from 0 : %d" % bb.search_first_unset(0)
    print "search first unset from 1 : %d" % bb.search_first_unset(1)
    print "search first unset from 2 : %d" % bb.search_first_unset(2)
    print "search first unset from 15 : %d" % bb.search_first_unset(15)
    print "search first unset from 55 : %d" % bb.search_first_unset(55)
    print "search first unset from 56 : %d" % bb.search_first_unset(56)
    print "search first unset from 59 : %d" % bb.search_first_unset(59)
    
    ba.unset(1)
    ba.unset(58)
    print ba
    
    buf=ba.save(0, test_size)
    for c in buf:
        print "%02X" % (ord(c),),
    print
    bb.load(buf, 0)
    print bb
    print 'count %d %d' % bb.count()
    
    for i in range(test_size-2):
        print bb.alloc(),
    print '\ncount %d %d' % bb.count()

#test_bit_array()
#sys.exit(1)
        
class ddumbfs:

    header_size=Header.header_size
    header_fmt='!Q'
    default_hash='SHA1'
    default_block_filename='ddfsblocks'
    default_index_filename='ddfsidx'
    cfg_filename='ddfs.cfg'
    root_dir='ddfsroot'
    special_dir=".ddumbfs"
    special_filenames=[ 'stats', 'stats0', 'reclaim']

    bheader_fmt='!8s'
    iheader_fmt='!8s'
    magic='DDUMBFS'

    def hash(self, block):
        if self.options.hash!='SHA1':
            raise RuntimeError, 'support only SHA1 hash for now'
        if hashlib: 
            return hashlib.sha1(block).digest()
        else:
            return sha.new(block).digest()

#    def hash2idx_old(self, hash):
#        """calculate the expected address of the hash in index, use first self.c_addr_bits 
#        bits of the hash and insert some free space to handle overflow"""
#        i=0
#        idx=0
#        bits=self.c_addr_bits
#        while bits>=8:
#            idx=idx*256+ord(hash[i])
#            bits-=8
#            i+=1
#        
#        if bits>0:
#            idx=(idx<<bits) + (ord(hash[i])>>(8-bits))
#
#        # scale it and provision to allow overflow 
#        # (order of * and / is important for int calculation)
#        idx=idx*self.c_block_count/(1<<self.c_addr_bits)*self.c_node_overflow/256
#        return idx

    _coefhash2idx=1.0/65536.0/65536.0/65536.0/65536.0
    
    def hash2idx(self, hash):
        """calculate the expected address of the hash in index using first bits 
        of the hash and insert some free space to handle overflow"""

        f=float(struct.unpack('!Q', hash[:8])[0])
        
        return int(f*self.c_block_count*self.c_node_overflow*self._coefhash2idx);


    def get_node_addr(self, baddr):
        """extract address from node"""
        addr=0
        for i in range(self.c_addr_size):
            addr=addr*256+ord(baddr[i])
        return addr

    def convert_addr(self, addr):
        """convert integer into string of self.c_addr_size length"""
        st=''
        for i in range(self.c_addr_size):
            st=chr(addr%256)+st
            addr=addr/256
        return st

    def set_node(self, idx, addr, hash):
        """convert integer into string of self.c_addr_size length"""
        n=idx*self.c_node_size
        i=n+self.c_addr_size
        while i>n:
            i-=1
            self.nodes[i]=chr(addr%256)
            addr=addr/256
        self.nodes[n+self.c_addr_size:n+self.c_node_size]=hash
        
    def search_free_node(self, idx, stop_before=0):
        """return idx of first free node""" 
        if stop_before==0:
            stop_before=self.c_node_count
        while idx<stop_before:
            addr=self.get_node_addr(self.nodes[idx*self.c_node_size:idx*self.c_node_size+self.c_addr_size])
            if addr==0:
                return idx
            idx+=1
        return -1 # no more free node
        
    @staticmethod
    def find_root(path, root_path=None):
        absp=os.path.abspath(path)
        if root_path:
            absr=os.path.abspath(root_path)
        else:
            absr=None
            
        head, tail=absp, None
        root=None
        while head!='/':
            # print head, tail, root, ddumbfs.root_dir
            head, tail=os.path.split(head)
            if tail==ddumbfs.root_dir:
                root=head
                if root==absr:
                    break
        
        if not root or not os.path.isfile(os.path.join(root, ddumbfs.cfg_filename)):
            root=None
        
        return root

    def __init__(self, root, options):
        self.root=root
        self.options=options

        self.rdir=os.path.join(self.root, self.root_dir)
        self.cfgfilename=os.path.join(self.root, self.cfg_filename)
        
        self.bfile=None
        self.ifile=None

        self.stat_attr=('stat_block_write', 'stat_block_write_try_next_block', 'stat_ghost_write', 'stat_block_write_try_next_node', 'stat_block_write_slide_block', 'stat_block_write_slide', 'stat_read_node_block')
        for attr in self.stat_attr:
            setattr(self, attr, 0)

        self.c_block_size=None
        self.c_index_block_size=NODE_BLOCK_SIZE
        if self.options.hash=='SHA1' or self.options.hash=='TIGER160':
            self.c_hash_size=20
        if self.options.hash=='TIGER':
            self.c_hash_size=24
        if self.options.hash=='TIGER128':
            self.c_hash_size=16
            
        self.c_node_overflow=None 
        
        self.c_block_count=None
        self.c_addr_size=None
        self.c_node_size=None

        self.c_nodes=None
        self.c_fblocks=None
        self.c_index_block_count=None
        self.usedblocks=None
        
    def init(self):
        """initialize a new ddumbfs in root directory"""
        if os.path.isdir(self.root):
            print 'directory already exists: %s' % self.root
        else:
            os.mkdir(self.root)

        if os.path.isdir(self.rdir):
            print 'directory already exists, remove and re-create: %s' % self.rdir
            shutil.rmtree(self.rdir, False)

        os.mkdir(self.rdir)

        # initialize special files
        special_dir=os.path.join(self.rdir, self.special_dir)
        if not os.path.isdir(special_dir):
            os.mkdir(special_dir)
            
        for filename in self.special_filenames:
            full_filename=os.path.join(special_dir, filename)
            f=open(full_filename, 'w')
            f.close()
            os.chmod(full_filename, 0444)
        
        blockfile=os.path.join(self.root, self.options.blockfile)
             
        self.c_partition_size=self.options.size
        if blockfile.startswith('/dev'):
            if not os.path.exists(self.options.blockfile):
                print 'block file not found: %s' % (self.options.blockfile, )
                sys.exit(1)
            # calculate device file size
            st=os.stat(self.options.blockfile)
            if not stat.S_ISBLK(st.st_mode):
                print 'not a block device: %s' % (self.options.blockfile, )
                sys.exit(1)
                
            self.bfile=open(self.options.blockfile, 'rb')
            bheader=self.bfile.read(self.options.block_size)
            (magic, )=struct.unpack(self.bheader_fmt, bheader[:struct.calcsize(self.bheader_fmt)])
            self.bfile.seek(0, os.SEEK_END)
            size=self.bfile.tell()
            self.bfile.close()
            
            if magic!=self.magic+'B':
                if raw_input('Do your really want to initialize this block device: %s (y/n)?' % (self.options.blockfile, ))!='y':
                    sys.exit(1)
            
            if not self.c_partition_size or self.c_partition_size>size:
                self.c_partition_size=size

        else:
            if not self.options.size:
                print 'You must specify a size !'
                sys.exit(1)
            self.c_partition_size=self.options.size

        if self.c_partition_size<1024**3:
            print 'Size must be >1Go, only %d ! ' % (self.c_partition_size, )
            sys.exit(1)
            
            
        self.c_block_size=self.options.block_size
        self.c_node_overflow=self.options.overflow

        # initialize block file                
        self.bfile=open(blockfile, 'wb+')
        bheader=struct.pack(self.bheader_fmt, self.magic+'B') # first block =header
        print '%r %r' % (self.c_block_size, len(bheader))  
        self.bfile.write(bheader+'\0'*(self.c_block_size-len(bheader)))
        self.bfile.close()
        print 'block file initialized: %s' % (blockfile, )


        self.c_block_count=self.c_partition_size/self.c_block_size
        
        _c_addr_bits, self.c_addr_size=bitstoaddress(self.c_block_count)

        self.c_node_size=self.c_addr_size+self.c_hash_size
        # calculate c_node_count including overflow and _lot_ of free space at end to avoid a DEADLY overflow 
        self.c_node_count=int(self.c_block_count*self.c_node_overflow+512*self.c_node_overflow)
        self.c_node_block_count=(self.c_node_count*self.c_node_size+self.c_index_block_size-1)/self.c_index_block_size
        self.c_node_count=self.c_node_block_count*self.c_index_block_size/self.c_node_size

        self.c_freeblock_offset=self.c_index_block_size # first block is reserved
        self.c_freeblock_size=(self.c_block_count+7)/8
        self.c_node_offset=self.c_freeblock_offset+(self.c_freeblock_size+self.c_index_block_size-1)/self.c_index_block_size*self.c_index_block_size
        self.c_index_size=self.c_node_offset+self.c_node_block_count*self.c_index_block_size
        self.c_index_block_count=self.c_index_size/self.c_index_block_size

        indexfile=os.path.join(self.root, self.options.indexfile)
             
        if indexfile.startswith('/dev'):
            if not os.path.exists(self.options.indexfile):
                print 'index file not found: %s' % (self.options.indexfile, )
                sys.exit(1)

            st=os.stat(self.options.indexfile)
            if not stat.S_ISBLK(st.st_mode):
                print 'not a block device: %s' % (self.options.indexfile, )
                sys.exit(1)
                
            self.ifile=open(self.options.indexfile, 'rb')
            iheader=self.ifile.read(self.c_index_block_size)
            (magic,)=struct.unpack(self.iheader_fmt, iheader[:struct.calcsize(self.iheader_fmt)])
            self.ifile.seek(0, os.SEEK_END)
            index_size=self.ifile.tell()
            self.ifile.close()
            if index_size<self.c_index_size:
                print 'Index device is too small: %d<%d' % (index_size, self.c_index_size)
                sys.exit(1)
            
            if magic!=self.magic+'I':
                if raw_input('Do your really want to initialize this index device: %s (y/n)?' % (self.options.indexfile, ))!='y':
                    sys.exit(1)

        # initialize index file                
        self.ifile=open(indexfile, 'w+')
        print 'initialize index file %s: %d bytes, %d nodes' % (indexfile, self.c_index_size, self.c_node_count)
        iheader=struct.pack(self.iheader_fmt, self.magic+'I')
        self.ifile.write(iheader+'\0'*(self.c_index_block_size-len(iheader)))
        # setup free block bit list
        zblock='\0'*self.c_index_block_size
        # first block is reserved
        self.ifile.write('\x80'+zblock[1:])
        for i in range((self.c_node_offset-self.c_freeblock_offset)/self.c_index_block_size-1):
            self.ifile.write(zblock)
        
        for i in range(self.c_node_block_count):
            self.ifile.write(zblock)
            #print '%x %d' % (self.ifile.tell(), len(zblock))
            
        assert self.ifile.tell()==self.c_index_size, 'index file wrong size %d<>%d' % (self.ifile.tell(), self.c_index_size)
        self.ifile.close()
        print 'index file %s initialized' % (indexfile,)
        
        cfgfilename=os.path.join(self.root, self.cfg_filename)

        cfgfile=open(cfgfilename, 'w+')
        print >>cfgfile, 'file_header_size: %d' % Header.header_size
        print >>cfgfile, 'hash: %s' % self.options.hash
        print >>cfgfile, 'hash_size: %d' % self.c_hash_size
        print >>cfgfile, 'index_block_size: %d' % self.c_index_block_size
        print >>cfgfile, 'node_overflow: %.2f' % self.c_node_overflow

        print >>cfgfile, 'partition_size: %d' % self.c_partition_size
        print >>cfgfile, 'block_size: %d' % self.c_block_size
        print >>cfgfile, 'block_count: %d' % self.c_block_count

        print >>cfgfile, 'addr_size: %d' %  self.c_addr_size
        print >>cfgfile, 'node_size: %d' % self.c_node_size
        print >>cfgfile, 'node_count: %d' % self.c_node_count
        print >>cfgfile, 'node_block_count: %d' % self.c_node_block_count

        print >>cfgfile, 'freeblock_offset: %d' % self.c_freeblock_offset
        print >>cfgfile, 'freeblock_size: %d' % self.c_freeblock_size
        print >>cfgfile, 'node_offset: %d' % self.c_node_offset
        print >>cfgfile, 'index_size: %d' % self.c_index_size
        print >>cfgfile, 'index_block_count: %d' % self.c_index_block_count
        
        print >>cfgfile, 'root_directory: %s' % self.root_dir
        print >>cfgfile, 'block_filename: %s' % self.options.blockfile
        print >>cfgfile, 'index_filename: %s' % self.options.indexfile
        cfgfile.close()
        print 'ddumbfs initialized in %s' %  self.root

        print
        print open(cfgfilename).read()

    def show_stat(self):
        for attr in self.stat_attr:
            print '%33s %6d' % (attr[5:], getattr(self, attr))
        
    def mount(self):
        if not os.path.isdir(self.rdir):  
            print 'invalid ddumbfs: directory missing: %s' % self.rdir
            sys.exit(1)

        if not os.path.isfile(self.cfgfilename):  
            print 'invalid ddumbfs: cfg file missing: %s' % self.cfgfilename
            sys.exit(1)

        # load .cfg file
        for line in open(self.cfgfilename):
            key, value=line.split(':')
            key=key.strip()
            value=value.strip()

            # print >>sys.stderr, '%30s %s' %(key, value)
            if key in ('block_filename', 'index_filename', 'root_directory'):
                value=value.strip('"')
            elif key in ('hash', ):
                value=value
            elif key in ('node_overflow', ):
                value=float(value)
            else:
                value=int(value)
                    
            # print 'c_%s=%r' % (key, value)
            setattr(self, 'c_'+key, value)

        # check block file or device 
        blockfile=os.path.join(self.root, self.c_block_filename)
        self.bfile=open(blockfile, 'rb', self.c_block_size)
        bheader=self.bfile.read(self.c_block_size)
        (magic, )=struct.unpack(self.bheader_fmt, bheader[:struct.calcsize(self.bheader_fmt)])
        self.bfile.close()
        if magic!=self.magic+'B':
            print >>sys.stderr, 'magic not  found: %s' % (blockfile, )
            sys.exit(1)
        
        # check index file or device 
        indexfile=os.path.join(self.root, self.c_index_filename)
        self.ifile=open(indexfile, 'rb')
        iheader=self.ifile.read(self.c_index_block_size)
        (magic, )=struct.unpack(self.iheader_fmt, iheader[:struct.calcsize(self.iheader_fmt)])
        self.ifile.close()
        if magic!=self.magic+'I':
            print >>sys.stderr, 'magic not found: %s' % (indexfile, )
            sys.exit(1)

        # open block file or device
        self.bfile=open(blockfile, 'rb+', self.c_block_size)

        # open index file or device
        self.ifile=open(indexfile, 'rb+', self.c_index_block_size)
        self.usedblocks_mmap=mmap.mmap(self.ifile.fileno(), self.c_freeblock_size, offset=self.c_freeblock_offset)
        # load free block list
        self.usedblocks=bit_array(self.c_block_count, buffer=self.usedblocks_mmap)
#        self.ifile.seek(self.c_freeblock_first*self.c_index_block_size)
#        for i in range(self.c_node_first-self.c_freeblock_first):
#            self.usedblocks.load(self.ifile.read(self.c_index_block_size), i*self.c_index_block_size*8)
        self.nodes=mmap.mmap(self.ifile.fileno(), self.c_node_block_count*self.c_index_block_size, offset=self.c_node_offset)
        
        print >>sys.stderr, 'mounted: %s' % (self.rdir, )

    def umount(self):
        # save block file size
        #self.bfile.seek(0)
        #bheader=struct.pack(self.bheader_fmt, self.magic+'B')  
        #self.bfile.write(bheader)
        # save free block list
        self.usedblocks_mmap.flush()
        self.usedblocks_mmap.close()
#        self.ifile.seek(self.c_freeblock_first*self.c_index_block_size)
#        for i in range(self.c_node_first-self.c_freeblock_first):
#            self.ifile.write(self.usedblocks.save(i*self.c_index_block_size*8, self.c_index_block_size*8))
        # close both
        
        self.nodes.flush()
        self.nodes.close()
        self.bfile.close()
        self.ifile.close()
        
    def search_node(self, bhash):
        node_idx0=node_idx=self.hash2idx(bhash)

        while node_idx<self.c_node_count:
            addr=self.get_node_addr(self.nodes[node_idx*self.c_node_size:node_idx*self.c_node_size+self.c_addr_size])
            if addr==0:
                return addr, node_idx-node_idx0
            else:
                nhash=self.nodes[node_idx*self.c_node_size+self.c_addr_size:(node_idx+1)*self.c_node_size]
                res=cmp(nhash, bhash)
                if res==0:
                    return addr, node_idx-node_idx0
                elif res>0:
                    return 0, node_idx-node_idx0
            
            node_idx+=1

    def store_new_block(self, block, bhash, node_idx):
        """store block into block file, update index and return address of the new block"""
        
        baddr=self.usedblocks.alloc()
        if baddr==-1:
            print 'ERROR, no more free block !'
            return -1
        
        #if (baddr+1)*self.c_block_size>self.bsize:
            # print 'bsize %d -> %d' % (self.bsize, (baddr+1)*self.c_block_size)
        #    self.bsize=(baddr+1)*self.c_block_size
            
        self.bfile.seek(baddr*self.c_block_size, os.SEEK_SET)
        self.bfile.write(block)
            
        self.set_node(node_idx, baddr, bhash) 
        # print 'store_new_block nidx=%d addr=%d' % (node_idx, baddr)
        return baddr

    def block_write(self, block):
        self.stat_block_write+=1
        bhash=self.hash(block)
        node_idx=self.hash2idx(bhash)
        # print 'write block try nidx=%d %s' % (node_idx, hexdump(bhash))

        while node_idx<self.c_node_count:
            addr=self.get_node_addr(self.nodes[node_idx*self.c_node_size:node_idx*self.c_node_size+self.c_addr_size])
            if addr==0:
                # the node is free, use it to store the new block
                baddr=self.store_new_block(block, bhash, node_idx)
                # print '       free nidx=%d baddr=%4d %s' % (node_idx, baddr, hexdump(bhash))
                return baddr, bhash
            else:
                # the node is not free, compare ?
                nhash=self.nodes[node_idx*self.c_node_size+self.c_addr_size:(node_idx+1)*self.c_node_size]
                res=cmp(nhash, bhash)
                if res==0:
                    # found the matching hash, the bloc is already known this is done
                    # print '      found nidx=%d baddr=%4d %s' % (node_idx, addr, hexdump(bhash))
                    self.stat_ghost_write+=1
                    return addr, bhash
                elif res>0:
                        # the new node must be inserted before existing nodes
                        # and nodes must slip until a free node is found
                        
                        # search for a free node
                        free_node_idx=self.search_free_node(node_idx+1)
                        if free_node_idx>0:
                            # just slip enough bytes
                            self.stat_block_write_slide+=1
                            self.nodes.move((node_idx+1)*self.c_node_size, node_idx*self.c_node_size,  (free_node_idx-node_idx)*self.c_node_size)
                            baddr=self.store_new_block(block, bhash, node_idx)
                            #print '     insert nidx=%d baddr=%4d %s' % (node_idx, baddr, hexdump(bhash))
                            return baddr, bhash
                        print 'Unexpected ERROR no free node found !!!'
                        sys.exit(1)

                elif res<0:
                    # the new node must go after, continue to search
                    # print '      collision try next'
                    self.stat_block_write_try_next_node+=1
            
            node_idx+=1

    def check_block_hash(self):
        """check match between block name and block hash"""
        # not used anymore

    def upload(self, source, destination):
        """upload one file to the ddumbfs"""
        try:
            if source=='-':
                if os.path.isdir(destination):
                    print 'destination is a directory and source as no name !'
                    return -1
                file_in=sys.stdin
                total=0
            else:
                file_in=open(source, 'rb')
                total=os.stat(source).st_size
        except IOError, e:
            print >>sys.stderr, e
        else:
            self.mount()
            total/=self.c_block_size
            
            if os.path.isdir(destination):
                destination=os.path.join(destination, os.path.basename(source))
            file_out=open(destination, 'wb')
            
            out=''
            header=Header()
            file_out.write(header.get())
            block=file_in.read(self.c_block_size)
            count=0
            while block:
                size=len(block)
                header.size+=size
                if size<self.c_block_size:
                    block+='\0'*(self.c_block_size-size)
                
                baddr, bhash=self.block_write(block)
                #print 'uplo', baddr
                # print hashlib.sha1(block).hexdigest(), baddr
    # ASX ATTN        
                file_out.write(self.convert_addr(baddr)+bhash)
                block=file_in.read(self.c_block_size)
                if count%100==0:
                    print >>sys.stderr,'upload %d/%d\r' % (count, total),
                count+=1
            print >>sys.stderr,'uploaded %d blocks' % (count, )
            file_out.seek(0)
            file_out.write(header.get())
            self.umount()
            if source!='-':
                file_in.close()
            self.show_stat()
            return 0

    def info(self, target):
        """return info about target file"""
        self.mount()

        if os.path.isdir(target):
            # print open(self.cfgfilename).read()
            print
            
            s, u=self.usedblocks.count()
            print 'block_count:     %9d  %5.1f%%' % (self.c_block_count, self.c_block_count*100.0/self.c_block_count, )
            print 'used_block:      %9d  %5.1f%%' % (s, s*100.0/self.c_block_count, )
            print 'free_block:      %9d  %5.1f%%' % (u, u*100.0/self.c_block_count, )
            bs, file_count, block_ref, block_dup, block_invalid, frag_count=self.block_accounting()
            acs, acu=bs.count()
            print 'file:            %9d' % (file_count, )
            print 'in_use_block     %9d  %5.1f%%' % (acs, acs*100.0/self.c_block_count, )
            print 'unused_block     %9d  %5.1f%%' % (acu, acu*100.0/self.c_block_count, )
            
            to_free, to_rehash=self.usedblocks.count_diff(bs)
            
            print 'block_to_free    %9d  %5.1f%%' % (to_free, (to_free)*100.0/s, )
            print 'block_to_rehash  %9d  %5.1f%%' % (to_rehash, (to_rehash)*100.0/acs, )
            print 'block_reference  %9d  %5.1f%%' % (block_ref, block_ref*100.0/acs, )
            print 'block_invalid    %9d  %5.1f%%' % (block_invalid, block_invalid*100.0/acs, )
            print 'block_fragment   %9d  %5.1f%%' % (frag_count, frag_count*100.0/acs, )

            used_node=0
            free_node=0
            
            node_direct=0
            node_next=0
            node_next_total=0
            node_next_block=0
            
            print '\ninspecting all nodes\n'
            node_idx=0
            while node_idx<self.c_node_count:
                if node_idx%100==0:
                    print '%6d/%d\r' % (node_idx, self.c_node_count),
                    sys.stdout.flush()

                addr=self.get_node_addr(self.nodes[node_idx*self.c_node_size:node_idx*self.c_node_size+self.c_addr_size])
                if addr==0:
                    free_node+=1
                else:
                    used_node+=1
                    nhash=self.nodes[node_idx*self.c_node_size+self.c_addr_size:(node_idx+1)*self.c_node_size]
                    idx=self.hash2idx(nhash)
                    if idx==node_idx:
                        node_direct+=1
                    else:
                        node_next+=1
                        node_next_total+=node_idx-idx
                        if idx*self.c_node_size/self.c_index_block_size!=node_idx*self.c_node_size/self.c_index_block_size:
                            node_next_block+=1
                node_idx+=1

            print 'node_count:      %9d  %5.1f%%' % (self.c_node_count, self.c_node_count*100.0/self.c_block_count, )

            print 'used_node:       %9d  %5.1f%%' % (used_node, used_node*100.0/self.c_block_count, )
            print 'free_node:       %9d  %5.1f%%' % (free_node-self.c_node_count+self.c_block_count, (free_node-self.c_node_count+self.c_block_count)*100.0/self.c_block_count, )
            print 'overflow_node:   %9d  %5.1f%%' % (self.c_node_count-self.c_block_count, (self.c_node_count-self.c_block_count)*100.0/self.c_block_count, )
            print 'node_direct:     %9d  %5.1f%%' % (node_direct, node_direct*100.0/self.c_block_count, )
            print 'node_next:       %9d  %5.1f%%' % (node_next, node_next*100.0/self.c_block_count, )
            print 'node_next_tot:   %9d' % (node_next_total, )
            print 'node_next_block: %9d  %5.1f%%' % (node_next_block, node_next_block*100.0/self.c_block_count, )
            
            
        else:
            file_in=open(target, 'rb')
            
            header=Header(file_in.read(self.header_size))
            print 'size=%d' % (header.size)
            count, ok=0, 0
            fragmentation, prev_addr=0, -1
            baddr=file_in.read(self.c_addr_size)
             
            while baddr:
                if count%100==0:
                    print '%6d\r' % (count,),
                    sys.stdout.flush()
                addr=self.get_node_addr(baddr)
                if prev_addr!=-1 and prev_addr+1!=addr and addr!=0:
                    fragmentation+=1
                if addr!=0:
                    prev_addr=addr
                self.bfile.seek(addr*self.c_block_size)
                block=self.bfile.read(self.c_block_size)
                bhash=self.hash(block)
                faddr, offset=self.search_node(bhash)
                
                free=not self.usedblocks.get(addr)
                if free:
                    print 'BFREE  #%6d addr=%6d' % (count, addr,)
                
                if faddr==0:
                    print 'NFOUND #%6d addr=%6d %s' % (count, addr, hexdump(bhash))
                elif faddr!=addr:
                    print 'WADDR  #%6d addr:%6d!=%6d off=%d %s' % (count, addr, faddr, offset, hexdump(bhash))
                else:
                    # print '       %6d %6d %s' % (count, addr, hexdump(bhash))
                    if not free:
                        ok+=1
                
                count+=1
                baddr=file_in.read(self.c_addr_size)
                
            print 'OK %d/%d  frag=%d (%.1f%%)' % (ok, count, fragmentation, 100.0*fragmentation/count)
                

        self.umount()

    def download(self, source, destination):
        """upload one file to the ddumfs"""
        self.mount()

        file_in=open(source, 'rb')
        if destination=='-':
            file_out=sys.stdout
        elif destination in (':md5', ':md5sum', ':md5check'):
            if hashlib:
                file_out=hashlib.md5()
            else:
                file_out=md5.new()
        else:
            if os.path.isdir(destination):
                destination=os.path.join(destination, os.path.basename(source))
            file_out=open(destination, 'wb')
        
        header=Header(file_in.read(self.header_size))
        print >>sys.stderr, 'download %d bytes into %s' % (header.size, destination)
        node=file_in.read(self.c_node_size)
        baddr=node[:self.c_addr_size]
        size=0
        while baddr:
            addr=self.get_node_addr(baddr)
            # print >>sys.stderr, hexdump(baddr), addr
            if addr==0:
                block='\0'*self.c_block_size
            else:
                self.bfile.seek(addr*self.c_block_size)
                block=self.bfile.read(self.c_block_size)
            if len(block)!=self.c_block_size:
                print >>sys.stderr, 'block %s wrong size %d' % (addr, len(block))
                sys.exit(1)
            length=self.c_block_size
            if size+length>header.size:
                length=header.size-size

            size+=length
            if isinstance(file_out, file):
                file_out.write(block[:length])
            else:
                file_out.update(block[:length])
            node=file_in.read(self.c_node_size)
            baddr=node[:self.c_addr_size]

        if not isinstance(file_out, file):
            hex=file_out.hexdigest()
            if destination==':md5check':
                if os.path.basename(source)==hex:
                    print 'OK ', hex
                else:
                    print 'ERR', hex
            else:
                print hex
                
        if size!=header.size:
            print >>sys.stderr, 'file size mismatch'

        self.umount()

    def block_accounting(self):
        bs=bit_array(self.c_block_count)
        file_count=block_ref=block_dup=block_invalid=0
        frag_count=0
        for root, dirs, fls in os.walk(self.rdir):
            # print root, dirs, fls
            for filename in fls:
                file_count+=1
                if file_count%10==0 or block_ref%1000==0:
                    print 'file=%6d  block=%9d\r' % (file_count, block_ref),
                    sys.stdout.flush()
                prev_addr=-1                    
                f=open(os.path.join(root, filename), 'rb')
                f.read(self.header_size)
                node=f.read(self.c_node_size)
                baddr=node[:self.c_addr_size]

                while baddr: 
                    block_ref+=1
                    addr=self.get_node_addr(baddr)

                    if prev_addr!=-1 and prev_addr+1!=addr and addr!=0:
                        frag_count+=1
                    if addr!=0:
                        prev_addr=addr
                    
                    if (addr<self.c_block_count):
                        if bs.set(addr):
                            block_dup+=1
                    else:
                        block_invalid+=1

                    node=f.read(self.c_node_size)
                    baddr=node[:self.c_addr_size]
                f.close()
        bs.set(0) # first is reserved
        return bs, file_count, block_ref, block_dup, block_invalid, frag_count
        
    def reclaim(self):
        self.mount()
        bs, file_count, block_count, ddup_count, invalid_count, frag_count=self.block_accounting()
        s, u=bs.count()
        print 'file=%d block=%d used=%d (%.1f%%) free=%d (%.1f%%) references=%d (%.1f%%) references=%d (%5.1f%%)' % (file_count, self.c_block_count, s, s*100.0/self.c_block_count, u, u*100.0/self.c_block_count, block_count, block_count*100.0/self.c_block_count, frag_count, frag_count*100.0/self.c_block_count)
        
        # free the node in index and let node go back to appropriate 
        # place if required
        node_null='\0'*self.c_node_size
        node_idx=0
        while node_idx<self.c_node_count:
            if node_idx%1000==0:
                print 'nodes %5.1f%%\r' % (node_idx*100.0/self.c_node_count, ),
                sys.stdout.flush()
                
            addr=self.get_node_addr(self.nodes[node_idx*self.c_node_size:node_idx*self.c_node_size+self.c_addr_size])
            if addr==0:
                """node is free"""
            else:

                chash=self.nodes[node_idx*self.c_node_size+self.c_addr_size:(node_idx+1)*self.c_node_size]
                cidx=self.hash2idx(chash)
                if node_idx<cidx:
                    print '*** ERROR ERROR node is before its insertion place: %d<%d ERROR ERROR' % (node_idx, cidx)
                    print 'try to continue'

                
                if bs.get(addr):
                    """node is not in the list of node to free"""
                else:
                    #print 'free nidx %d addr=%d' % (node_idx, addr)
                    # reset this node
                    self.nodes[node_idx*self.c_node_size:(node_idx+1)*self.c_node_size]=node_null
                    # try to relocate next nodes
                    idx=node_idx+1
                    while idx<self.c_node_count:
                        addr=self.get_node_addr(self.nodes[idx*self.c_node_size:idx*self.c_node_size+self.c_addr_size])
                        if addr==0:
                            # this node is already empty don't go further
                            #print 'node nidx %d empty, stop' % (idx, )
                            break
                        if not bs.get(addr):
                            # node must be freed too, do it now 
                            self.nodes[idx*self.c_node_size:(idx+1)*self.c_node_size]=node_null
                            #print 'node nidx %d must be free too' % (idx, )
                        else:
                            chash=self.nodes[idx*self.c_node_size+self.c_addr_size:(idx+1)*self.c_node_size]
                            cidx=self.hash2idx(chash)
                            if cidx==idx:
                                # node in good place, don't go further 
                                #print 'node nidx %d in good place, stop' % (idx, )
                                break
                            else:
                                if idx<cidx:
                                    print 'ERROR ERROR node is before its insertion place: %d<%d ERROR ERROR' % (idx, cidx)
                                    print 'try to continue'
                                    sys.exit(1)
                                    break
                                
                                # search a free node for insertion 
                                fidx=self.search_free_node(cidx, idx)
                                #print 'relocate node nidx %d to %d (ideal place is %d)' % (idx, fidx, cidx)
                                assert fidx!=-1, 'no free node found, impossible'
                                # move idx to fidx
                                self.nodes.move(fidx*self.c_node_size, idx*self.c_node_size,  self.c_node_size)
                                self.nodes[idx*self.c_node_size:(idx+1)*self.c_node_size]=node_null
                        idx+=1
                    # don't increment node_idx, because nodes have moved up and current node is a new node
                    continue
            node_idx+=1


        s, u=self.usedblocks.count()
        print 'before used=%d free=%d' % (s, u, )
        self.usedblocks.bwand(bs)
        s, u=self.usedblocks.count()
        print 'after  used=%d free=%d' % (s, u, )
        
        self.umount()
        
    
def main():
    parser=parser=OptionParser(version='%%prog %s' % __version__ )
    parser.set_usage('%prog [options] command [arg]\n\n'
                      '\tinit rootdir\n'
                      '\treclaim rootdir\n'
                      '\tcheck_block_hash rootdir\n' 
                      '\tcheck target\n'
                      '\tupload source destination\n'
                      '\tdownload source destination\n')
    
    parser.add_option("-r", "--root", dest="root", help="root of the file system", metavar="DIR")
#    parser.add_option("-d", "--debug", dest="debug", action="store_true", default=False, help="diplay debugging message")
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="increase verbosity")
    parser.add_option("-f", "--force", dest="force", action="store_true", default=False, help="overwrite existing blocks")
    parser.add_option("-i", "--index", dest="indexfile", default=ddumbfs.default_index_filename, help="specify the index filename or device")
    parser.add_option("-b", "--block", dest="blockfile", default=ddumbfs.default_block_filename, help="specify the block filename or device")
    parser.add_option("-H", "--hash", dest="hash", default=ddumbfs.default_hash, help="specify the hash, in SHA1, TIGER128, TIGER160, TIGER", metavar="HASH")
    parser.add_option("-s", "--size",  dest="size", help="the size of block file")
    parser.add_option("-B", "--block-size", default="64k", dest="block_size", help="the block size (default is 64k)", metavar="BLOCK_SIZE")
    parser.add_option("-o", "--overflow",  type="float", default=1.3, dest="overflow", help="the overflow factor (default is 1.3)", metavar="OVERFLOW")

    cmd_options, cmd_args=parser.parse_args(sys.argv)
    
    if len(cmd_args)==0:
        parser.error('missing "command"')
        
    command=cmd_args[1]

    if cmd_options.hash not in ('SHA1', 'TIGER128', 'TIGER160', 'TIGER'):
            parser.error('-H option, must be in SHA1, TIGER128, TIGER160, TIGER')

    if cmd_options.overflow<=1.0 or  10<cmd_options.overflow:
            parser.error('-o option, 1.0 < overflow <= 10.0')
          
    if cmd_options.size:
        try:
            cmd_options.size=friendly_size(cmd_options.size)
        except ValueError:
            parser.error('-s option, bad size format')
            
    if cmd_options.block_size:
        try:
            cmd_options.block_size=friendly_size(cmd_options.block_size)
        except ValueError:
            parser.error('-B option, bad size format')
            
    if command in ('upload', 'download'):
        if len(cmd_args)!=4:
            parser.error('"%s" requires source and destination' % (command, ))
        source=cmd_args[2]
        destination=cmd_args[3]

        root=None
        if command=='upload':
            root=ddumbfs.find_root(destination, cmd_options.root)
            if not root:
                parser.error('"root" mismatch')
        elif command=='download':
            root=ddumbfs.find_root(source, cmd_options.root)
            if not root:
                parser.error('"root" mismatch')
                
        if not cmd_options.root:              
            cmd_options.root=root
            
        fs=ddumbfs(cmd_options.root, cmd_options)
        if command=='upload':
            fs.upload(source, destination)
        elif command=='download': 
            fs.download(source, destination)

    elif command in ('init', 'check_block_hash', 'reclaim'):
        if len(cmd_args)!=3:
            parser.error('"%s" requires target' % (command, ))
        dir=cmd_args[2]
        if command!='init' and not os.path.isdir(dir):
            parser.error('not a directory: %s' % (dir, ))
            
        fs=ddumbfs(dir, cmd_options)
        if command=='init':
            fs.init()
        elif command=='check_block_hash':
            fs.check_block_hash()
        elif command=='reclaim':
            fs.reclaim()

    elif command in ('info' ):
        if len(cmd_args)!=3:
            parser.error('"%s" requires target' % (command, ))
        target=cmd_args[2]
        root=ddumbfs.find_root(target, cmd_options.root)
        if not root:
            parser.error('"root" mismatch')
        if not cmd_options.root:              
            cmd_options.root=root
        fs=ddumbfs(cmd_options.root, cmd_options)
        if command=='info':
            fs.info(target)
        
    else:
        parser.error('unknown command: %s' % command)


main()
sys.exit(0)

