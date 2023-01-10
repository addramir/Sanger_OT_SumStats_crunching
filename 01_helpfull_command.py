import pandas as pd
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
import numpy as np

spark = SparkSession.builder.getOrCreate()

gwas_list=!gsutil ls gs://genetics-portal-dev-sumstats/unfiltered/gwas

gene_index=spark.read.parquet("gs://genetics-portal-dev-data/22.09.0/outputs/lut/genes-index")
gene_index=gene_index.toPandas()

variant_index=spark.read.parquet("gs://genetics-portal-dev-data/22.09.0/outputs/lut/variant-index")

thr=5e-8
window_sig=1e6
window_gene=1e6
path2save="gs://genetics-portal-dev-sumstats/tmp/yt4-filtered-sumstats/"

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


gw=gwas_list[1]

for gw in gwas_list[1:3]:
	print(gw)
	gwas=spark.read.parquet(gw)
	gwas=gwas.withColumn("lexa1a2",lex_orderUDF(f.col("ref"),f.col("alt")))
	gwas=gwas.withColumn("snpid",f.concat_ws("_",f.col("chrom"),f.col("pos"),f.col("lexa1a2")))
	sig=gwas.filter(gwas.pval<=thr)
	L=gwas.filter("snpid is NULL")
	if sig.count()>0 :
		for row in sig.rdd.collect():
    		#print(row)
    		chrom=row.chrom
    		pos=int(row.pos)
    		l=gwas.filter(f.when((gwas.chrom==chrom)&(gwas.pos<=(pos+window_sig))&(gwas.pos>=(pos-window_sig)),1).otherwise(0)==1)
    		L=L.union(l)
    		L=L.distinct()
	for i in range(0,gene_index.shape[0]):
		chrom=gene_index.iloc[i,1]
		print(chrom)
		start=gene_index.iloc[i,3]-window_gene
		end=gene_index.iloc[i,8]+window_gene
		l=gwas.filter(f.when((gwas.chrom==chrom)&(gwas.pos<=end)&(gwas.pos>=start),1).otherwise(0)==1)
		
		L=L.union(l)
		L=L.distinct()
	L=L.join(VI,["snpid"])
	L=L.distinct()
	L=L.toPandas()
	gwn=L.at[0,"study_id"]
	L.to_csv(path2save+gwn+".csv",index=False)

print("Done!")




