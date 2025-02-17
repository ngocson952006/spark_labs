from datetime import date, timedelta, datetime

import scrapy


class KHLotterySpider(scrapy.Spider):
    """
    Represents a Scrapy spider for scraping lottery results for a specific province in Cambodia. The spider iterates through
    all dates in a specified year and scrapes lottery result information from a predefined website.

    This spider extracts prize titles, results, and order from the HTML table structure of the website. It yields the results
    as structured dictionaries for further processing.

    :ivar name: Name of the Scrapy spider.
    :type name: str
    :ivar DATE_DETECT_PREFIX: Prefix used in URLs to detect the date information.
    :type DATE_DETECT_PREFIX: str
    :ivar PROVINCE_CODE: Code representing the province for which the lottery results are being scraped.
    :type PROVINCE_CODE: str
    :ivar year: Year for which the spider scrapes lottery data.
    :type year: int
    """
    name = "kh_lottery_spider"

    DATE_DETECT_PREFIX = 'ngay-'
    PROVINCE_CODE = 'KH79'

    def __init__(self, year=None, *args, **kwargs):
        super(KHLotterySpider, self).__init__(*args, **kwargs)
        self.year = int(year)

    def start_requests(self):
        base_url = "https://xskt.com.vn/xskh/ngay-{day}-{month}-{year}"
        start_date = date(self.year, 1, 1)  # Start of 2024
        end_date = date(self.year, 12, 31)  # End of 2024
        delta = timedelta(days=1)  # To iterate over each day

        current_date = start_date
        while current_date <= end_date:
            # Format URL with current date
            formatted_url = base_url.format(
                day=current_date.day,
                month=current_date.month,
                year=current_date.year
            )
            yield scrapy.Request(url=formatted_url, callback=self.parse)
            current_date += delta  # Move to the next day

    def parse(self, response):
        """
        Parses the response to extract prize information from an HTML table.

        This method processes the HTML response to extract prize details such as prize
        title, results, and order for a specific date and province. If the expected
        table rows are not found in the response, the processing is skipped for that
        date.

        :param response: A scrapy.http.Response object containing the HTML response to parse.
        :type response: scrapy.http.Response
        :return: A generator yielding dictionaries representing the parsed prize data.
        :rtype: generator
        """
        url = response.request.url
        date_part = url.split('/')[-1].replace(self.DATE_DETECT_PREFIX, '')
        date_object = datetime.strptime(date_part, "%d-%m-%Y").date()

        print(f'Processing result for date: {date_object} for {self.PROVINCE_CODE}')
        expected_tr_data = response.css("table#KH0 tr")
        if not expected_tr_data:
            print(f'No data for date: {date_object}. Skip this date')
            return  # Skip processing if no rows are found in the table
        for row in expected_tr_data:
            # Extract the prize title (from text inside the <td>)
            prize_title = row.css("td:first-child::text").get()  # Use first-child to find the text in the first <td>

            prize_code_elements = row.css(
                "td:nth-child(2) em::text, "
                "td:nth-child(2) p *::text, "  # Includes text within <p>, <br>, or child tags
                "td:nth-child(2)::text"
            ).getall()
            # Extract the prize code(s) (found in <em> or <p> within the second column)
            prize_results = ' '.join(prize_code_elements).strip()  # Combine and clean extra spaces

            # Extract the prize order (contained in the third column)
            prize_order = row.css("td:nth-child(3)::text").get()

            # Print or process the extracted data
            if prize_title and prize_results:
                # You can also structure the data into a dictionary for further processing
                yield {
                    'province_code': self.PROVINCE_CODE,
                    'date': date_part,
                    'title': prize_title,
                    'prize_codes': prize_results,
                    'prize_order': prize_order
                }
