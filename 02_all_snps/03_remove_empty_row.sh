cd /mnt/disks/gwas/

path2csv=/mnt/disks/gwas/magma_csvs/
path2save=/mnt/disks/gwas/magma_csvs_no_empty/


cust_func(){

path2csv=/mnt/disks/gwas/magma_csvs/
path2save=/mnt/disks/gwas/magma_csvs_no_empty/

awk -F ['\t'] '$1!=""' "$path2csv"/"$1".csv  > "$path2save"/"$1".csv

}

export -f cust_func

cat csv_magma_list.txt | parallel -j 16 cust_func {}