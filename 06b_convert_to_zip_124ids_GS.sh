cd ~/projects/filter_SS/00_GS_with_SS

gwas_list=$(gsutil ls gs://genetics-portal-dev-sumstats/tmp/yt4-filtered-GS/)

path2csv=~/projects/filter_SS/00_GS_with_SS/01_text
path2zip=~/projects/filter_SS/00_GS_with_SS/02_zip

path2zip_gs=gs://genetics-portal-dev-sumstats/tmp/yt4-filtered-GS-zip/

for gw in $gwas_list
do
	studyid=${gw:54}
	studyid=${studyid%?}
	echo $studyid
	cat ~/projects/filter_SS/header.txt > "$path2csv"/"$studyid".csv
	gsutil cat "$gw"*.csv >> "$path2csv"/"$studyid".csv
	zip "$path2zip"/"$studyid".zip "$path2csv"/"$studyid".csv
	gsutil cp "$path2zip"/"$studyid".zip $path2zip_gs
done

