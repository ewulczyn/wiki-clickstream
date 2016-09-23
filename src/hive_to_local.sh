
table=2016_08_en_unresolved
AGG_TABLE=${table}
FUNNY_NULL_TSV=${table}_clickstream_funny_null.tsv
FINAL_TSV=${table}_clickstream.tsv



cd ~/clickstream/data

rm -r ${AGG_TABLE}
rm ${FUNNY_NULL_TSV}
rm ${FINAL_TSV}
rm ${FINAL_TSV}.gz

hadoop fs -copyToLocal /user/hive/warehouse/clickstream.db/${AGG_TABLE} ${AGG_TABLE}
cat header.txt ${AGG_TABLE}/* > ${FUNNY_NULL_TSV}
sed 's/\\N//g' ${FUNNY_NULL_TSV} > ${FINAL_TSV}
gzip ${FINAL_TSV}

rm -r ${AGG_TABLE}
rm ${FUNNY_NULL_TSV}



cd ~/clickstream/data
rm ${FINAL_TSV}
rm ${FINAL_TSV}.gz

scp stat1002.eqiad.wmnet:clickstream/data/${FINAL_TSV}.gz /Users/ellerywulczyn/clickstream/data/${FINAL_TSV}.gz
