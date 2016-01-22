import os
import sys
import string
import subprocess
from itertools import islice
from functools import partial
from multiprocessing.dummy import Pool, Lock
from cdis_pipe_utils import df_util
from cdis_pipe_utils import pipe_util
from cdis_pipe_utils import time_util

def do_pool_commands(cmd, uuid, engine, logger, lock = Lock()):
    logger.info('running mutect2_pon chunk call: %s' % cmd)
    output = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output_stdout = output.communicate()[1]
    with lock:
        logger.info('contents of output=%s' % output_stdout.decode().format())
        df = time_util.store_time(uuid, cmd, output_stdout, logger)
        df['cmd'] = cmd
        unique_key_dict = {'uuid': uuid, 'cmd': cmd}
        table_name = 'time_mem_MuTect2_Panel_Of_Normal_chunk_call_processes'
        df_util.save_df_to_sqlalchemy(df, unique_key_dict, table_name, engine, logger)
        logger.info('completed mutect2_pon chunk call: %s' % str(cmd))
    return output.wait()

def multi_commands(uuid, cmds, thread_count, engine, logger):
    pool = Pool(int(thread_count))
    output = pool.map(partial(do_pool_commands, uuid=uuid, engine=engine, logger=logger), cmds)
    return output

def fai_chunk(fai_path, blocksize):
  seq_map = {}
  with open(fai_path) as handle:
    head = list(islice(handle, 25))
    for line in head:
      tmp = line.split("\t")
      seq_map[tmp[0]] = int(tmp[1])
    for seq in seq_map:
        l = seq_map[seq]
        for i in range(1, l, blocksize):
            yield (seq, i, min(i+blocksize-1, l))

def mutect2_pon_cmd_template(gatk_path, ref, fai_path, blocksize, normal_cram, cosmic, dbsnp, output_base):
  template = string.Template("java -Djava.io.tmpdir=/tmp/job_tmp -d64 -jar ${GATK_PATH} -T MuTect2 -R ${REF} -L ${REGION} -I:tumor ${NORMAL_CRAM} --cosmic ${COSMIC} --dbsnp ${DBSNP} --artifact_detection_mode -o ${OUTPUT_BASE}.${BLOCK_NUM}")
  for i, block in enumerate(fai_chunk(fai_path, blocksize)):
    cmd = template.substitute(
                              dict(
                                   REF = ref,
                                   REGION = '%s:%s-%s' % (block[0], block[1], block[2]),
                                   BLOCK_NUM = i),
                                   GATK_PATH = gatk_path,
                                   NORMAL_CRAM = normal_cram,
                                   COSMIC = cosmic,
                                   DBSNP = dbsnp,
                                   OUTPUT_BASE = output_base
    )
    yield cmd, "%s.%s.mt2pon.vcf" % (output_base, i)

def pon(uuid, cram_path, thread_count, reference_fasta_path, cosmic_path, fai_path, blocksize, known_snp_vcf_path, engine, logger):
  step_dir = os.path.join(os.getcwd(), 'pon')
  os.makedirs(step_dir, exist_ok=True)
  cram_name = os.path.basename(cram_path)
  cram_base, cram_ext = os.path.splitext(cram_name)
  merge_dir = os.getcwd()
  os.makedirs(merge_dir, exist_ok=True)
  out_pon_vcf = os.path.join(merge_dir, tb_base) + '_pon.vcf'
  logger.info('out_pon_vcf=%s' % out_pon_vcf)
  if pipe_util.already_step(step_dir, cram_name + '_MuTect2_PON', logger):
    logger.info('already completed step `MuTect2 Panel Of Normal calling` of: %s' % cram_path)
  else:
    logger.info('running step `MuTect2 Panel Of Normal calling` of: %s' % cram_path)
    home_dir = os.path.expanduser('~')
    gatk_path = os.path.join(home_dir, 'tools', 'GenomeAnalysisTK.jar')
    cmds = list(mutect2_pon_cmd_template(
                                   gatk_path = gatk_path,
                                   ref = reference_fasta_path,
                                   fai_path = fai_path,
                                   blocksize = blocksize,
                                   normal_cram = cram_path,
                                   cosmic = cosmic_path,
                                   dbsnp = known_snp_vcf_path,
                                   output_base = os.path.join(step_dir, 'output'))
    )
    outputs = multi_commands(uuid, list(a[0] for a in cmds), thread_count, engine, logger)
    first = True
    with open (out_pon_vcf, "w") as ohandle:
      for cmd, out in cmds:
        with open(out) as handle:
          for line in handle:
            if first or not line.startswith('#'):
              ohandle.write(line)
        first = False
    pipe_util.create_already_step(step_dir, cram_name + '_MuTect2_PON', logger)
    logger.info('completed running step `MuTect2 Panel Of Normal calling` of: %s' % cram_path)
  return
