
import os
import sys

from cdis_pipe_utils import df_util
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util

def pon(uuid, cram_path, thread_count, reference_fasta_path, cosmic_path, intervals_path, known_snp_vcf_path, engine, logger):
  step_dir = os.getcwd()
  cram_name = os.path.basename(cram_path)
  cram_base, cram_ext = os.path.splitext(cram_name)
  out_pon_vcf = os.path.join(step_dir, cram_base) + '_pon.vcf'
  logger.info('out_pon_vcf=%s' % out_pon_vcf)
  if pipe_util.already_step(step_dir, cram_name + '_MuTect2_PON', logger):
    logger.info('already completed step `MuTect2 Panel Of Normal calling` of: %s' % cram_path)
  else:
    logger.info('running step `MuTect2 Panel Of Normal calling` of: %s' % cram_path)
    home_dir = os.path.expanduser('~')
    gatk_path = os.path.join(home_dir, 'tools', 'GenomeAnalysisTK.jar')
    cmd = ['java', '-Djava.io.tmpdir=/tmp/job_tmp', '-d64', '-jar', gatk_path, '-nct', str(thread_count), '-T', 'MuTect2', '-R', reference_fasta_path, '-I:tumor', cram_path, '--cosmic', cosmic_path, '--dbsnp', known_snp_vcf_path, '--artifact_detection_mode', '-L', intervals_path, '-o', out_pon_vcf]
    output = pipe_util.do_command(cmd, logger)

    #store time/mem
    df = time_util.store_time(uuid, cmd, output, logger)
    df['out_pon_vcf'] = out_pon_vcf
    df['cram_path'] = cram_path
    df['thread_count'] = thread_count
    table_name = 'time_mem_gatk_mutect2_pon'
    unique_key_dict = {'uuid': uuid, 'cram_path': cram_path, 'thread_count': thread_count, 'out_pon_vcf': out_pon_vcf}
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
    pipe_util.create_already_step(step_dir, cram_name + '_MuTect2_PON', logger)
    logger.info('completed running step `MuTect2 Panel Of Normal calling` of: %s' % cram_path)
  return
