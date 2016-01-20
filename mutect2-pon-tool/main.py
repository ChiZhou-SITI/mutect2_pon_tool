#!/usr/bin/env python

import argparse
import logging
import os
import sys
import sqlalchemy
from cdis_pipe_utils import pipe_util

import tools.RealignerTargetCreator as RealignerTargetCreator
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
    parser.add_argument('-indel', '--known_indel_vcf_path',
                        required = False,
                        help='Reference INDEL path.',
    )
    parser.add_argument('-snp', '--known_snp_vcf_path',
                        required = False,
                        help='Reference SNP path.',
    )
    parser.add_argument('-cos', '--known_cosmic_path',
                        required = False,
                        help='Reference COSMIC path.',
    )
    parser.add_argument('-b', '--bam_path',
                        required = False,
                        action="append",
                        help = 'Source bam path.',
    )
    parser.add_argument('-i', '--intervals_path',
                        required = False
                        action = "append",
                        help = 'Target intervals',
    )
    parser.add_argument('-v', '--vcf_path',
                        required = False
                        action = "append",
                        help = 'Individual VCF path',
    )
    parser.add_argument('-j', '--thread_count',
                        required = False,
                        type = is_nat,
                        help = 'Maximum number of threads for execution.',
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

    logger = pipe_util.setup_logging('gatk_' + tool_name, args, uuid)
    engine = pipe_util.setup_db(uuid)

    hostname = os.uname()[1]
    logger.info('hostname=%s' % hostname)


    if tool_name == 'RealignerTargetCreator':
        bam_path = pipe_util.get_param(args, 'bam_path')[0]
        known_indel_vcf_path = pipe_util.get_param(args, 'known_indel_vcf_path')
        reference_fasta_path = pipe_util.get_param(args, 'reference_fasta_path')
        thread_count = pipe_util.get_param(args, 'thread_count')
        RealignerTargetCreator.rtc(uuid, bam_path, thread_count, reference_fasta_path, known_indel_vcf_path, engine, logger)
    elif tool_name == 'mutect2_pon_tool':
        bam_path = pipe_util.get_param(args, 'bam_path')[0]
        known_snp_vcf_path = pipe_util.get_param(args, 'known_snp_vcf_path')
        intervals_path = pipe_util.get_param(args, 'intervals_path')[0]
        cosmic_path = pipe_util.get_param(args, 'cosmic_path')
        reference_fasta_path = pipe_util.get_param(args, 'reference_fasta_path')
        thread_count = pipe_util.get_param(args, 'thread_count')
        mutect2_pon_tool.pon(uuid, bam_path, thread_count, reference_fasta_path, cosmic_path, intervals_path, known_snp_vcf_path, engine, logger)
    elif tool_name == 'CombineVariants':
        vcf_path_list = pipe_util.get_param(args, 'vcf_path')
        reference_fasta_path = pipe_util.get_param(args, 'reference_fasta_path')
        thread_count = pipe_util.get_param(args, 'thread_count')
        CombineVariants.combinevcf(uuid, vcf_path_list, reference_fasta_path, engine, logger)
    else:
        sys.exit('No recognized tool was selected')


if __name__ == '__main__':
    main()
