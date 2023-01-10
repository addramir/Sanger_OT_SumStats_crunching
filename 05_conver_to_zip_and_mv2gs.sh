cd ~/projects/filter_SS/01_filtered_data

gwas_list=$(gsutil ls gs://genetics-portal-dev-sumstats/tmp/yt4-filtered-sumstats/)

for gw in $gwas_list
do
	studyid=${gw:60}
	studyid=${studyid%?}
	echo $studyid
	cat ~/projects/filter_SS/header.txt > "$studyid".csv
	gsutil cat "$gw"*.csv >> "$studyid".csv
	zip ~/projects/filter_SS/02_filtered_data_zip/"$studyid".zip "$studyid".csv
	gsutil cp ~/projects/filter_SS/02_filtered_data_zip/"$studyid".zip gs://genetics-portal-dev-sumstats/tmp/yt4-filtered-zip/
done

