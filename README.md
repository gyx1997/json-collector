# json-collector
Collect json objects and convert to structured data with SQL query support.


This tool is used for filtering duplicate Json data and save them for advanced query.
The motivation that I create this tool is that I need to crawl paginated data from api like posts (threads) of forums; I also don't want 
duplicate post(s) may be caught if api with parameter `page` are simpled used, especially for crawlers with low frequency 
(in this situation, newly replied posts will be displayed on first page as usually they are sorted by reply time). 
So this tool would be helpful when crawling medium amount data and save it for further processing. In future a server will be 
developed based on this tool, to accept objects from crawlers and save the data in backend database (it may use MySQL backend for larger data).

As it is for my personal purpose, tests are not fully conducted. Pull requests on fixes, or new features are welcome.
It is also welcome to expand this tool on other data collection scenarios. 

### Features
+ Accept nested json objects with conversion to structured data.
+ Detect and skip duplicate data
+ Support data persistence with built-in SQLite3
+ Query data with various format with SQL language



### Benchmark on Insertion
As it is just a tool for my own purpose, it is just benchmarked on data collected from `eastmoney.com` 
(online trader/information provider of Chinese/HK stock market). The data is based on posts on some stockbar 
(internal BBS on `eastmoney.com` for stock buyers to communicate).
It includes ~240k objects, while ~30k are duplicates. In total raw json files are ~200MiB.

Environment:
Microsoft Windows 11 22H2

CPython 3.11

Intel Core i7 10700 CPU (4.60GHz Boost) 

64GiB Memory

Intel Optane 800 Series 110GiB SSD with USB3.1 Interface and ~400MiB/s Sequential Read/Write

Insert: ~29000 obj/sec (In Memory), ~21000 obj/sec (file on SSD)

Query: Less than 1 seconds when exporting all data to CSV with sorting on indexed keys.



### Usage
#### System Requirement

Python 3.10 or above. If you want to export the collected data to pandas.DataFrame, 
package `pandas` needs to be installed.

#### Example
Put `collector.py` and `fields.py` under the directory of your own script. 
```Python
from collector import JsonDataCollector
# String, Int, Float and DataTime are types that currently supported
from fields import String, Int, DateTime, Float

# Define your fields here
# Specifying fields to be captured.
fields = [
    Int("postId", "post_id"),
    String("userName", "post_user.user_nickname"),  # use '.' to capture nested json attributes
    Int("deactive", "post_user.user_extendinfos.deactive"),
    String("postTitle", "post_title"),
    Int("postClicks", "post_click_count"),
    Int("postComments", "post_comment_count"),
    DateTime("postPublishTime",
             "post_publish_time",
             value_converter=lambda x: datetime.datetime.fromisoformat(x)),
    String("stockBarCode", "stockbar_code", ), 
    Int("userId", "user_id"),
]
# Create collector instance
c = JsonDataCollector("eastMoney",
                      fields=fields,
                      unique_keys=[0],  # Specify the column (aka field, or key) for duplicate detection (usually ID field)
                      in_memory=True,  # For data persistence, set False to use file storage. Insert performance will be decreased.
                      sorted_keys=[[6]],  # Keys for creating index. This will speedup performance for queries, but slow down the insert operations.
                      append=True,   # New objects will be appended to the backend sqlite database if True. Otherwise, an empty table will be created (old data will be lose!).
                      ignore_duplicates=True)  # Set True to ignore duplicate objects according to unique_keys.
# Add data
while True:
    data = get_data()  # get dict data from your data source...
    c.add(data)

    # Exit your loop at some time...

# Query data
# Example: query post title which has been appeared more than 5 times, ordered by click count descendingly.
objects = c.query(
    sql="SELECT * FROM (SELECT `postTitle` as `PostTitle`, COUNT(1) as `PostCount`, SUM(`postClicks`) as `ClickCount`, SUM(`postComments`) as `ReplyCount` FROM eastMoney  GROUP BY `PostTitle`) WHERE `PostCount` > 5 ORDER BY `ClickCount` DESC")
```

Note: It is just a simple tool to collect data for simple purpose. Thread-safety (e.g., concurrent insert operations) is not implemented.

