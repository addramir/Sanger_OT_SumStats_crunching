import pandas as pd
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f
import numpy as np

spark = SparkSession.builder.getOrCreate()

gene_index=spark.read.parquet("gs://genetics-portal-dev-data/22.09.0/outputs/lut/genes-index")
gene_index=gene_index.toPandas()



def lex_order(str1,str2):
	str1=str1.upper()
	str2=str2.upper()
	if str1<str2:
 		out=str1+"_"+str2
	else:
 		out=str2+"_"+str1
	return out	

lex_orderUDF = f.udf(lambda z1,z2: lex_order(z1,z2),f.StringType())

variant_index=spark.read.parquet("gs://genetics-portal-dev-data/22.09.0/outputs/lut/variant-index")
variant_index=variant_index.withColumn("lexa1a2",lex_orderUDF(f.col("ref_allele"),f.col("alt_allele")))
variant_index=variant_index.withColumn("snpid",f.concat_ws("_",f.col("chr_id"),f.col("position"),f.col("lexa1a2")))
variant_index=variant_index.withColumn("ingene",f.lit(0))


window_gene=5e5
path2save="gs://genetics-portal-dev-sumstats/tmp/yt4-filtered-sumstats/"

list_of_chr= gene_index["chr"].unique()
L=variant_index.filter("snpid is NULL")

for chrom in list_of_chr:
	print("chromosome: "+chrom)
	gi_chrom=gene_index.loc[gene_index["chr"]==chrom,]
	vi=variant_index.filter(variant_index.chr_id==chrom)
	for i in range(0,gi_chrom.shape[0]):
		start=gi_chrom["start"].iloc[i]-window_gene
		end=gi_chrom["end"].iloc[i]+window_gene
		vi=vi.withColumn("ingene", f.when(((vi.position<=end)&(vi.position>=start))|(vi.ingene==1),1).otherwise(0))
		if i%100==0:
			print(i)
		L=L.union(vi)

	l=variant_index.filter(f.when((variant_index.chr_id==chrom)&(variant_index.position<=end)&(variant_index.position>=start),1).otherwise(0)==1)
	L=L.union(l)
	if i%100==0:
		print(i)
		L=L.distinct()







for i in range(0,gene_index.shape[0]):
	chrom=gene_index.iloc[i,1]
	start=gene_index.iloc[i,3]-window_gene
	end=gene_index.iloc[i,8]+window_gene
	vi=vi.withColumn("ingene", f.when(((vi.chr_id==chrom)&(vi.position<=end)&(vi.position>=start))|(vi.ingene==1),1).otherwise(0))
	if i%100==0:
		print(i)

	l=variant_index.filter(f.when((variant_index.chr_id==chrom)&(variant_index.position<=end)&(variant_index.position>=start),1).otherwise(0)==1)
	L=L.union(l)
	if i%100==0:
		print(i)
		L=L.distinct()

	



L=L.join(VI,["snpid"])
L=L.distinct()
L=L.toPandas()
gwn=L.at[0,"study_id"]
L.to_csv(path2save+gwn+".csv",index=False)

print("Done!")




