setwd("~/Projects/saving_gwas_filtered_to_bucket/")

libray(data.table)

GS=fread("GS_l2G.txt",data.table=F)
si=fread("study_index.csv",data.table=F)
fss=fread("list_of_otg_gwas.txt",data.table=F,header=F)
fss=fss[,1]

#
efos_ss=si$trait_efos
efos_ss=gsub(efos_ss,pattern="['",replacement = "",fixed = T)
efos_ss=gsub(efos_ss,pattern="']",replacement = "",fixed = T)
si$trait_efos=efos_ss
#

#
y=GS$trait_info.ontology
y=y[!is.na(y)]    
y=unique(y)
efos=y

#
fss=gsub(fss,pattern = 'gs://genetics-portal-dev-sumstats/unfiltered/gwas/',replacement = "")
fss=gsub(fss,pattern='.parquet/',replacement = "")
table(fss%in%si$study_id)

#
si_fss=si[si$study_id%in%fss,]
efos_ss=si_fss$trait_efos
efos_ss=unique(efos_ss)

table(efos%in%efos_ss)
efos_with_ss=efos[efos%in%efos_ss]
dim(GS)
table(GS$trait_info.ontology%in%efos_with_ss)

#####

selection_of_pow_gwas=function(x)
{
  y=si_fss[si_fss$trait_efos==x,]
  i=1
  n=NULL
  for (i in 1:nrow(y)){
    if (!is.na(y$n_cases[i])){
      prev=y$n_cases[i]/y$n_initial[i]
      Neff=4*prev*(1-prev)*y$n_initial[i]
    } else {
      Neff=y$n_initial[i]
    }
    n=c(n,Neff)  
  }
  y[which.max(n),"study_id"]
}
l=sapply(efos_with_ss,FUN=selection_of_pow_gwas)
l[1]

GS=cbind(GS,the_biggest_GWAS_id=l[GS$trait_info.ontology])

fwrite(x=GS,file="GS_with_biggest_GWASid.txt",sep="\t")

ids=unique(GS$the_biggest_GWAS_id[!is.na(GS$the_biggest_GWAS_id)])
fwrite(x=cbind(ids=ids),file="list_of_GS_biggest_GWASids.txt",col.names = F,row.names = F,sep="\t")
