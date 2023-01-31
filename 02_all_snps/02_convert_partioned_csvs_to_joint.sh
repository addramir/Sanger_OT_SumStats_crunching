cd /mnt/disks/gwas/

path2csv=/mnt/disks/gwas/magma_prtnd_csvs/
path2save=/mnt/disks/gwas/magma_csvs/

ls $path2csv > csv_magma_list.txt

cust_func(){

path2csv=/mnt/disks/gwas/magma_prtnd_csvs/
path2save=/mnt/disks/gwas/magma_csvs/

cat /mnt/disks/gwas/magma_header.txt > "$path2save"/"$1".csv
cat "$path2csv"/"$1"/*.csv >> "$path2save"/"$1".csv

}

export -f cust_func

cat csv_magma_list.txt | parallel -j 16 cust_func {}