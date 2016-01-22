#!/usr/bin/env python

import argparse
import logging
import os
import sys
import sqlalchemy
from cdis_pipe_utils import pipe_util

import tools.mutect2_pon_tool as mutect2_pon_tool
import tools.CombineVariants as CombineVariants


def is_nat(x):
    '''
    Checks that a value is a natural number.
    '''
    if int(x) > 0:
        return int(x)
    raise argparse.ArgumentTypeError('%s must be positive, non-zero' % x)

def main():
    parser = argparse.ArgumentParser('GATK MuTect2 Panel Of Normal creation')

    # Logging flags.
    parser.add_argument('-d', '--debug',
        action = 'store_const',
        const = logging.DEBUG,
        dest = 'level',
        help = 'Enable debug logging.',
    )
    parser.set_defaults(level = logging.INFO)

    # Required flags.

    parser.add_argument('-r', '--reference_fasta_path',
                        required = False,
                        help = 'Reference fasta path.',
    )
    parser.add_argument('-rf', '--reference_fasta_fai',
                        required = False,
                        help = 'Reference fasta fai path.',
    )
    parser.add_argument('-snp', '--known_snp_vcf_path',
                        required = False,
                        help='Reference SNP path.',
    )
    parser.add_argument('-cos', '--cosmic_path',
                        required = False,
                        help='Reference COSMIC path.',
    )
    parser.add_argument('-b', '--cram_path',
                        required = False,
                        action="append",
                        help = 'Source cram path.',
    )
    parser.add_argument('-v', '--vcf_path',
                        required = False,
                        action="append",
                        help = 'Individual VCF path',
    )
    parser.add_argument('-j', '--thread_count',
                        required = False,
                        type = is_nat,
                        help = 'Maximum number of threads for execution.',
    )
    parser.add_argument('-bs', '--Parallel_Block_Size',
                        type = is_nat,
                        default = 50000000,
                        required = False,
                        help = 'Parallel Block Size',
    )
    parser.add_argument('-u', '--uuid',
                        required = True,
                        help = 'analysis_id string',
    )
    parser.add_argument('--tool_name',
                        required = True,
                        help = 'gatk tool'
    )

    args = parser.parse_args()
    tool_name = args.tool_name
    uuid = args.uuid
    thread_count = str(args.thread_count)
    Parallel_Block_Size = str(args.Parallel_Block_Size)

    logger = pipe_util.setup_logging('gatk_' + tool_name, args, uuid)
    engine = pipe_util.setup_db(uuid)

    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)


    if tool_name == 'mutect2_pon_tool':
        cram_path = pipe_util.get_param(args, 'cram_path')[0]
        known_snp_vcf_path = pipe_util.get_param(args, 'known_snp_vcf_path')
        cosmic_path = pipe_util.get_param(args, 'cosmic_path')
        reference_fasta_path = pipe_util.get_param(args, 'reference_fasta_path')
        thread_count = pipe_util.get_param(args, 'thread_count')
        fai_path = pipe_util.get_param(args, 'reference_fasta_fai')
        blocksize = pipe_util.get_param(args, 'Parallel_Block_Size')
        mutect2_pon_tool.pon(uuid, cram_path, thread_count, reference_fasta_path, cosmic_path, fai_path, blocksize, known_snp_vcf_path, engine, logger)
    elif tool_name == 'CombineVariants':
        vcf_path_list = pipe_util.get_param(args, 'vcf_path')
        reference_fasta_path = pipe_util.get_param(args, 'reference_fasta_path')
        thread_count = pipe_util.get_param(args, 'thread_count')
        CombineVariants.combinevcf(uuid, vcf_path_list, reference_fasta_path, engine, logger)
    else:
        sys.exit('No recognized tool was selected')


if __name__ == '__main__':
    main()
