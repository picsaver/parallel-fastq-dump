#! /usr/bin/env python3
import sys
import os
import re
import glob
import shutil
from multiprocessing.dummy import Pool as ThreadPool
import subprocess
import argparse
import logging
# import tempfile

__version__ = '0.6.7'

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.DEBUG)
class CustomFormatter(argparse.ArgumentDefaultsHelpFormatter,
                      argparse.RawDescriptionHelpFormatter):
    pass

desc = 'parallel fastq-dump2 wrapper, extra args will be passed through'
epi = """DESCRIPTION:
Example: parallel-fastq-dump2.py --sra-id SRR2244401 --splitN 10 --threads 4 --outdir out/ --split-files --gzip
"""

parser = argparse.ArgumentParser(description=desc, epilog=epi,
                                 formatter_class=CustomFormatter)
argparse.ArgumentDefaultsHelpFormatter
parser.add_argument('-s','--sra-id', help='SRA id', action='append')
parser.add_argument('-t','--threads', help='number of threads', default=1, type=int)
parser.add_argument('-P','--splitN', help='number of n_pieces', default=1, type=int)
parser.add_argument('-O','--outdir', help='output directory', default='.')
parser.add_argument('-T', '--tmpdir', help='temporary directory', default='/tmp')
parser.add_argument('-N','--minSpotId', help='Minimum spot id', default=1, type=int)
parser.add_argument('-X','--maxSpotId', help='Maximum spot id', default=None, type=int)
parser.add_argument('-V', '--version', help='shows version', action='store_true', default=False)

global awk_script
awk_script =  os.path.join(os.path.dirname(__file__), "fixed_fastq.sh")


    
def download_continued(start_SpotId, end_SpotId, srr_id, outdir_prefix = "./", extra_args = [], retry = 1):
    def packedLastOutput(srr_id, outdir_prefix, retry = 1):
        global awk_script
        outdir = "%s-%02d" %(outdir_prefix, retry)
        # target_files = list(filter(lambda f: re.search("^%s" %srr_id, f), os.listdir(outdir)))
        target_files = glob.glob("%s/%s_*" %(outdir, srr_id))
        is_gzip = bool(re.search('gz$', target_files[0]))
        cmd = ['bash', str(awk_script), "-z" ] + [target_files]
        p = subprocess.Popen(cmd, stdout = subprocess.PIPE)
        exit_code = p.poll()
        if exit_code != 0:
            logging.error('fixed file failed in path {}'.format(outdir))
        return(p.stdout.read().strip())

    if retry > 1:
        # get last retry output EndSpot as this try StartSpot
        start_SpotId = packedLastOutput(srr_id, outdir_prefix, retry = retry -1)
        if not start_SpotId:
            logging.warning('{}/try {}:can not get end Spot for another try start Spot'.format(outdir_prefix, retry-1))
            sys.exit(1)
        else:
            start_SpotId = int(start_SpotId) + 1
        if start_SpotId > int(end_SpotId): # actualy finished
            return retry
        logging.info('{}/try {}: get end Spot for another try start Spot {}'.format(outdir_prefix, retry-1, start_SpotId))

    outdir = "%s-%02d" %(outdir_prefix, retry)
    if not os.path.exists(outdir): os.mkdir(outdir)

    cmd = ['fastq-dump', '-N', str(start_SpotId), '-X', str(end_SpotId),
           '-O', outdir] + extra_args + [srr_id]
    logging.info('CMD: {}'.format(' '.join(cmd)))
    p = subprocess.Popen(cmd)
    # exit_code = p.poll()
    exit_code = p.wait()
    if exit_code != 0:
        logging.warning('{}/try {}: fastq-dump error! exit code: {}'.format(outdir, retry, exit_code))
        return download_continued(start_SpotId = start_SpotId, 
                                 end_SpotId = end_SpotId, 
                                 srr_id = srr_id, 
                                 outdir_prefix = outdir_prefix, 
                                 extra_args = extra_args, 
                                 retry = retry + 1)
        #sys.exit(1)
    logging.info('finish {} => try {}'.format(outdir_prefix, retry ))
    return retry

def download_continued_submit(dict_args):
    return download_continued(
        start_SpotId = dict_args['start_SpotId'], 
        end_SpotId = dict_args['end_SpotId'],
        srr_id = dict_args['srr_id'],
        outdir_prefix = dict_args['outdir_prefix'] if 'outdir_prefix' in dict_args else "./",
        extra_args = dict_args['extra_args'] if 'extra_args' in dict_args else [],
        retry = dict_args['retry'] if 'retry' in dict_args else 1
    )
    
def split_blocks(start, end, n_pieces):
    total = (end-start+1)
    avg = int(total / n_pieces)
    out = []
    last = start
    for i in range(0,n_pieces):
        out.append([last,last + avg-1])
        last += avg
        if i == n_pieces-1: out[i][1] += total % n_pieces
    return out

def get_spot_count(sra_id):
    """
    Get spot count via sra-stat
    Parameters
    ----------
    sra_id : str
        SRA ID
    """
    cmd = ['sra-stat', '--meta', '--quick', sra_id]
    logging.info('CMD: {}'.format(' '.join(cmd)))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()    
    txt = stdout.decode().rstrip().split('\n')
    total = 0
    try:
        for l in txt:
            total += int(l.split('|')[2].split(':')[0])
    except IndexError:
        msg = 'sra-stat output parsing error!'
        msg += '\n--sra-stat STDOUT--\n{}'
        msg += '\n--sra-stat STDERR--\n{}'
        etxt = stderr.decode().rstrip().split('\n')
        raise IndexError(msg.format('\n'.join(txt), '\n'.join(etxt)))
    return total

def partition(f, l):
    r = ([],[])
    for i in l:
        if f(i):
            r[0].append(i)
        else:
            r[1].append(i)
    return r

def is_sra_file(path):
    """
    Determine whether path is SRA file
    parameters
    ----------
    path : str
        file path
    """
    f = os.path.basename(path)
    if f.lower().endswith('.sra'): return True
    if 'SRR' in f.upper(): return True
    if 'ERR' in f.upper(): return True
    if 'DRR' in f.upper(): return True
    return False



def pfd(args, srr_id, extra_args):
    """
    Parallel fastq dump
    Parameters
    ----------
    args : dict
        User-provided args
    srr_id : str
        SRR ID
    extra_args : dict
        Extra args
    """
    # tmp_dir = tempfile.TemporaryDirectory(prefix='pfd_',dir=args.tmpdir)
    tmp_dir = os.path.join(args.tmpdir, "tmp_%s" %os.path.basename(srr_id))
    if (not os.path.exists(tmp_dir)):
        os.makedirs(tmp_dir)
    logging.info('tempdir: {}'.format(tmp_dir))

    n_spots = get_spot_count(srr_id)
    logging.info('{} spots: {}'.format(srr_id,n_spots))

    # minSpotId cant be lower than 1
    start = max(args.minSpotId, 1)
    # maxSpotId cant be higher than n_spots
    end = min(args.maxSpotId, n_spots) if args.maxSpotId is not None else n_spots

    blocks = split_blocks(start, end, args.splitN)
    logging.info('blocks: {}'.format(blocks))
    
    # ps = []
    Pools = ThreadPool(args.threads)
    download_args = []
    for i in range(0,args.splitN):
        d = os.path.join(tmp_dir, "%02d" %i)
        # os.mkdir(d)
        # cmd = ['fastq-dump', '-N', str(blocks[i][0]), '-X', str(blocks[i][1]),
        #        '-O', d] + extra_args + [srr_id]
        # p = subprocess.Popen(cmd)
        # ps.append(p)
        #start_SpotId, end_SpotId, srr_id, outdir_prefix = "./", extra_args = [], retry 
        download_args.append({'start_SpotId':blocks[i][0], 
                                'end_SpotId':blocks[i][1], 
                                'srr_id':srr_id, 
                                'outdir_prefix':d, 
                                'extra_args':extra_args, 
                                'retry':1}
                            )
        logging.info('submit: {} {}-{}'.format(d,blocks[i][0], blocks[i][1]))
    # print(download_args[0])
    reslut = Pools.map(download_continued_submit, download_args)
    Pools.close()
    Pools.join()
    logging.info('each split try count: \n\t{}'.format(
        "\n\t".join(map(lambda x: "split %02d try %s times" %(x[0], str(x[1])), zip(range(args.splitN), reslut))
    )))
    

def main():
    """
    Main interface
    """
    args, extra = parser.parse_known_args()
    args.threads = min(args.threads, args.splitN)
    if args.version:
        print('parallel-fastq-dump : {}'.format(__version__))
        subprocess.Popen(['fastq-dump', '-V']).wait()
        sys.exit(0)

    elif args.sra_id:
        extra_srrs, extra_args = partition(is_sra_file,extra)        
        args.sra_id.extend(extra_srrs)
        logging.info('SRR ids: {}'.format(args.sra_id))
        logging.info('extra args: {}'.format(extra_args))

        # output directory
        if not os.path.isdir(args.outdir) and args.outdir != '.':
            os.makedirs(args.outdir)
        # temp directory
        if (not os.path.exists(args.tmpdir)):
            os.makedirs(args.tmpdir)
            
        # fastq dump
        for si in args.sra_id:
            pfd(args, si, extra_args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == '__main__':
    main()
