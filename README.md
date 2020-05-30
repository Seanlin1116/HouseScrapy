# HouseScrapy
## 環境設定

環境 -Windows 10 Anaconda 的 python 

安裝 scrapy 網路爬蟲請到 Anaconda prompt輸入`pip install scrapy`

scrapy 的 framework 創造 到cmd 輸入`scrapy startproject "Name"`, 此次project我命名為"HouseScrapy"

Spark的設定，可到官網 https://spark.apache.org/downloads.html 下載最新版本

由於Spark本身也會需要安裝java JDK，**請注意**建議安裝jdk-8(版本太新可能無法支援)

另外需安裝winutils.exe 將其放在\winutils\bin 底下 

*設定環境變數
 
 *Add %JAVA_HOME% = C:\jdk
 
 *Add %SPARK_HOME% = C:\spark
 
 *Add %HADOOP_HOME% = C:\winutils

*PATH變數增加
 
 *%JAVA_HOME%\bin
 
 *%SPARK_HOME%\bin

spark 執行指令則為 `submit-spark name.py`
## 目標
內政部不動產時價登錄網 http://plvr.land.moi.gov.tw/DownloadOpenData

1.抓取 內政部不動產時價登錄網 中位於 【 臺北市 新北市 桃園市 臺中市 高雄市 】 的 【 不動產買賣 】 資料。

【 非本期下載 】【 CSV 格式 】【 資料內容 發布日期 108 年第 2 季 】


2.使用 Spark 合併 資料集， 以下列條件從篩選出結果
【 主要用途 】 為 【 住家用 】

【 建物型態 】 為 【 住宅大樓 】

【 總樓層數 】 需 【 大於等於十三層 】

3.使用 Spark 將步驟 2 的篩選結果，轉換成 Json 格式 產生 【 result part1.json 】 和
【result part2.json 】 五個城市隨機放入兩個檔案 


## 執行檔案/說明
### 網路爬蟲
Scrapy 會創造一個framework 包含四個 .py檔(item, middlewares, pipeline, setting)等 和一個spider的folder檔。

此spider的folder檔裡有一個 **plvr.py** 為此次**主要爬蟲**的檔案，

處理項目包含，AJAX的處理和萃取網頁相對應的條件參數。

**item.py**和**pipline.py**和**setting.py**主要則為負責資料串接，路徑，命名的調整。

OUTPUT 會存在Downloads檔裡，【 a_lvr_land_a 】【 b_lvr_land_a 】 【 e_lvr_land_a 】
【f_lvr_land_a 】 【 h_lvr_land_a 】

詳情請參閱 plvr.py 和item.py和 pipeline.py和 setting.py。

**請注意**執行 `python plvr.py` 時，由於是framework,檔案並不能單獨存在執行。
### Spark
**HouseScrapy_spark.py**為此次spark讀取檔案和轉換JSON，

主要的資料處理都是在Spark的 DataFrame 上處理，

包含partition, 中文數字轉換/日期轉換的 UDF。

OUTPUT 存在result.json的檔案裡。

詳情請參閱**HouseScrapy_spark.py。