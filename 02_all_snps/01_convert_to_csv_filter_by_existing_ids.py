import pandas as pd
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.window import Window
import numpy as np

global spark
spark = (
    SparkSession.builder
    .master('local[*]')
    .config('spark.driver.memory', '15g')
    .appName('spark')
    .getOrCreate()
)
#spark = SparkSession.builder.getOrCreate()


#######
path2parquet="/mnt/disks/gwas/raw_20230120/"
gwas_list=!ls /mnt/disks/gwas/raw_20230120
#gwas_list=gwas_list[1:]

path2save="/mnt/disks/gwas/magma_prtnd_csvs/"
gwas_list_exists=!ls /mnt/disks/gwas/magma_prtnd_csvs

#gwas_list_exists=gwas_list_exists[1:]

gwl1=[i.replace(path2parquet,'') for i in gwas_list]
gwl1=[i.replace('.parquet','') for i in gwl1]

gwl2=[i.replace(path2save,'') for i in gwas_list_exists]
gwl2=[i.replace('/','') for i in gwl2]

gwl=list(set(gwl1)-set(gwl2))

#######

variant_index=spark.read.parquet("/mnt/disks/gwas/variant-index")

def lex_order(str1,str2):
	str1=str1.upper()
	str2=str2.upper()
	if str1<str2:
 		out=str1+"_"+str2
	else:
 		out=str2+"_"+str1
	return out	

lex_orderUDF = f.udf(lambda z1,z2: lex_order(z1,z2),f.StringType())

variant_index=variant_index.withColumn("lexa1a2",lex_orderUDF(f.col("ref_allele"),f.col("alt_allele")))
variant_index=variant_index.withColumn("snpid",f.concat_ws("_",f.col("chr_id"),f.col("position"),f.col("lexa1a2")))
VI=variant_index.select(f.col("chr_id_b37"),f.col("position_b37"),f.col("rs_id"),f.col("snpid"))

#####

gw=gwl[0]
i=0

#cts=['rs_id','chrom','pos','ref','alt','beta','se','pval',
#'n_total','n_cases','eaf','chr_id_b37','position_b37']

cts=['rs_id','pval','n_total']

for i,gw in enumerate(gwl):
	print("N "+str(i)+": "+gw)
	gw=path2parquet+gw+'.parquet'
	gwas=spark.read.parquet(gw)
	gwas=gwas.withColumn("lexa1a2",lex_orderUDF(f.col("ref"),f.col("alt")))
	gwas=gwas.withColumn("snpid",f.concat_ws("_",f.col("chrom"),f.col("pos"),f.col("lexa1a2")))
	gwas=gwas.join(VI,["snpid"]).distinct()
	gwas=gwas.filter((gwas.eaf>=0.001)&(gwas.eaf<=0.999))
	gwas=gwas.select(cts)
	gwn=gwl[i]
	gwn=gwn.replace('.parquet','')
	gwas.write.csv(path2save+gwn,sep="\t")

