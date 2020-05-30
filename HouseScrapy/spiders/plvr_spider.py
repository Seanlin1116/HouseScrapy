#import modules
import scrapy
from bs4 import BeautifulSoup
from HouseScrapy.items import HousescrapyItem

# init a spider named plvr 
class PlvrSpider(scrapy.Spider):
    name = "plvr"
#extracting Ajax info
    def start_requests(self):
        urls = [
            'https://plvr.land.moi.gov.tw/DownloadSeason_ajax_list'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
# extracting conditional corresponding details 
    def parse(self, response):
        soup = BeautifulSoup(response.text, 'lxml')

        season = ""
        formatType = ""
        cityCodeList = []
        
        for option in soup.find_all('option'):
            if (option.text == "108年第2季"):
                season = option.get("value")
                print("Found the season:", season)

            if (option.text == "CSV 格式"):
                formatType = option.get("value")
                print("Found the formatType:", formatType)

        for advDownloadClass in soup.find_all("tr", class_="advDownloadClass"):
            if (advDownloadClass.find("font").get_text() == "臺北市" or 
                advDownloadClass.find("font").get_text() == "新北市" or 
                advDownloadClass.find("font").get_text() == "桃園市" or
                advDownloadClass.find("font").get_text() == "臺中市" or
                advDownloadClass.find("font").get_text() == "高雄市"):
                for inputValue in advDownloadClass.find_all("input", class_="checkBoxGrp landTypeA"):
                    cityCodeList.append(inputValue.get("value"))

        for cityCode in cityCodeList:
            file_url = "https://plvr.land.moi.gov.tw/DownloadSeason?season=" + season + "&type=" + formatType + "&fileName="+ cityCode +".csv"
            file_url = response.urljoin(file_url)
            item = HousescrapyItem()
            item['file_urls'] = [file_url]
            yield item


  