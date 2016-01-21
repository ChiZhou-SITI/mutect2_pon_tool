
import os
import sys

from cdis_pipe_utils import df_util
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util

def combinevcf(uuid, vcf_path_list, reference_fasta_path, thread_count, engine, logger):
  step_dir = os.getcwd()
  output_pon_vcf = os.path.join(step_dir, uuid) + '_PON.vcf'
  if pipe_util.already_step(step_dir, uuid + '_CombineVariants', logger):
    logger.info('already completed step `CombineVariants` of: %s' % vcf_path_list)
  else:
    logger.info('running step `CombineVariants` of: %s' % vcf_path_list)
    home_dir = os.path.expanduser('~')
    gatk_path = os.path.join(home_dir, 'tools/GenomeAnalysisTK.jar')
    cmd = ['java', '-Djava.io.tmpdir=/tmp/job_tmp', '-d64', '-jar', gatk_path, '-T', 'CombineVariants', '-nt', str(thread_count), '-R', reference_fasta_path, '-minN 2', '--setKey "null"', '--filteredAreUncalled', '--filteredrecordsmergetype KEEP_IF_ANY_UNFILTERED', '-o', output_pon_vcf]
    for vcf_path in vcf_path_list:
      cmd.extend(['-V', vcf_path])
    output = pipe_util.do_command(cmd, logger)
    df = time_util.store_time(uuid, cmd, output, logger)
    df['output_pon_vcf'] = output_pon_vcf
    table_name = 'time_mem_gatk_CombineVariants'
    unique_key_dict = {'uuid': uuid, 'output_pon_vcf': output_pon_vcf}
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    pipe_util.create_already_step(step_dir, uuid + '_CombineVariants', logger)
    logger.info('completed running step `CombineVariants` of: %s' % bam_path_list)
  return
