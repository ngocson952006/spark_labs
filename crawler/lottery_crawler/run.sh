year=2007
scrapy crawl kh_lottery_spider -a year=$year -o kh79_$year.csv
cp kh79_$year.csv ../../csv_files/lottery_data
rm kh79_$year.csv