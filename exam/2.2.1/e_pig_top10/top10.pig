top10input = load './input' AS (word:chararray, count:int);
top10sorted = ORDER top10input BY count DESC;
top10sortedandlimited = LIMIT top10sorted 10;
STORE top10sortedandlimited into './result';
