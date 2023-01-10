library(jsonlite)
# Give the input file name to the function.
myData <- fromJSON(file="Projects/saving_gwas_filtered_to_bucket/list_of_gwas_l2g.json")
library(jsonlite)
library(dplyr)

lines <- readLines("Projects/saving_gwas_filtered_to_bucket/list_of_gwas_l2g.json")
lines <- lapply(lines, fromJSON)
lines <- lapply(lines, unlist)
x <- bind_rows(lines)

y=x$association_info.otg_id
y=y[!is.na(y)]    
y=unique(y)

library(data.table)
fwrite(x=cbind(y=y),file="Projects/saving_gwas_filtered_to_bucket/list_of_GS_otgid.txt",col.names = F,row.names = F)

fwrite(x=x,file="Projects/saving_gwas_filtered_to_bucket/GS_l2G.txt",col.names = T,row.names = F)

y=x$trait_info.ontology
y=y[!is.na(y)]    
y=unique(y)


fwrite(x=cbind(y=y),file="Projects/saving_gwas_filtered_to_bucket/list_of_EFOs.txt",col.names = F,row.names = F)
