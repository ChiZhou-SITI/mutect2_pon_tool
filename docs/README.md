##Normal-only calling for panel of normals creation
java
  -jar GenomeAnalysisTK.jar \
  -T MuTect2 \
  -R reference.fasta \
  -I:tumor normal1.bam \
  [--dbsnp dbSNP.vcf] \
  [--cosmic COSMIC.vcf] \
  --artifact_detection_mode \
  [-L targets.interval_list] \
  -o output.normal1.vcf
