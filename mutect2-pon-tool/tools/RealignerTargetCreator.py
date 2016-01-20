
import os
import sys

from cdis_pipe_utils import df_util
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util

def rtc(uuid, bam_path_list, thread_count, reference_fasta_path, known_indel_vcf_path, engine, logger):
  step_dir = os.getcwd()
  out_intervals_path = uuid + '.intervals'
  logger.info('out_intervals_path=%s' % out_intervals_path)
  if pipe_util.already_step(step_dir, uuid + '_RealignerTargetCreator', logger):
    logger.info('already completed step `RealignerTargetCreator` of: %s' % bam_path_list)
  else:
    logger.info('running step `RealignerTargetCreator` of: %s' % bam_path_list)
    home_dir = os.path.expanduser('~')
    gatk_path = os.path.join(home_dir, 'tools', 'GenomeAnalysisTK.jar')
    cmd = ['java', '-Djava.io.tmpdir=/tmp/job_tmp', '-d64', '-jar', gatk_path, '-nt', str(thread_count), '-T', 'RealignerTargetCreator', '-R', reference_fasta_path, '-known', known_indel_vcf_path, '-o', out_intervals_path]
    for bam_path in bam_path_list:
      cmd.extend(['-I', bam_path])
    logger.info('cmd=%s' % cmd)
    output = pipe_util.do_command(cmd, logger)

    #store time/mem
    df = time_util.store_time(uuid, cmd, output, logger)
    df['intervals_path'] = out_intervals_path
    df['thread_count'] = str(thread_count)
    table_name = 'time_mem_gatk_realignertargetcreator'
    unique_key_dict = {'uuid': uuid, 'thread_count': str(thread_count), 'out_intervals_path': out_intervals_path}
    df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)

    pipe_util.create_already_step(step_dir, uuid + '_RealignerTargetCreator', logger)
    logger.info('completed running step `RealignerTargetCreator` of: %s' % bam_path_list)
  return
