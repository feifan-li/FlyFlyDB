# Group 25 Project - The Nosql Database: FlyFlyDB
## The file structure:
The DB/ directory is where all the databases, tables and records will be actually stored. The CLI, parser and storage engine are implemented under different directories. Further categorizations of functions are embodied in different sub-directories. The globals/ directory contain global information. The "main.go" is the entry point for building and executing the FlyFlyDB.
## The Easiest Approach to Launch this Database:
### just execute the "main.exe" file if you don't have Golang environment, but note that downloading the entire project is still required:
```
./main.exe
```
## Alternative Approach to Install and Launch this Database:
### First, make sure you have Golang installed, the highly suggested version is ```go1.19.5```
### Second, install the following helper golang libraries:
```
	github.com/google/uuid v1.4.0
	google.golang.org/protobuf v1.31.0
```
### Third, we have provided a Makefile for your convenience, if the Makefile works for you, then simply type the following command in your terminal to launch FlyFlyDB:
```
    make flyflydb
```
### If the Makefile does not work for you, just simply run the following in your terminal to launch this DB:
```
    go run main.go
```
# FlyFlyDB CLI commands syntax
## Once you successfully launch the database, you will see our CLI, we have provided all kinds of supported CLI commands for you to test:
### create a database:
```Fly
create:{"database":"youtube"};
```
### switch to a database:
```Fly
use:{"database":"youtube"};
```
### create tables:
```Fly
create:{"table":"category",
        "partition_key":"string country_code",
        "sort_key":"int32 id",
        "fields":["string name"],
        "partitions":"2"};
```
```Fly
create:{"table":"video",
        "partition_key":"string country_code",
        "sort_key":"string id",
        "fields":["string title","string channel_title",
            "int32 category_id","string publish_time",
            "int64 views","int64 likes","int64 dislikes"],
        "partitions":"2"};
```
### insert into category
```Fly
insert:{"table":"category",
        "values":["country_code=US","id=1","name=Film & Animation"]};
```
```Fly
insert:{"table":"category",
        "values":["country_code=US","id=15","name=Pets & Animals"]};
```
### insert into video
```Fly
insert:{"table":"video",
        "values":["country_code=US",
            "id=2kyS6SvSYSE",
            "title=WE WANT TO TALK ABOUT OUR MARRIAGE",
            "channel_title=CaseyNeistat",
            "category_id=22",
            "publish_time=2017-11-13T17:13:01.000Z",
            "views=748374","likes=57527","dislikes=2966"]
        };
```
```Fly
insert:{"table":"video",
        "values":["country_code=US",
            "id=gHZ1Qz0KiKM",
            "title=2 Weeks with iPhone X",
            "channel_title=iJustine",
            "category_id=28",
            "publish_time=2017-11-13T19:07:23.000Z",
            "views=119180","likes=9763","dislikes=511"]
        };
```
### select
```Fly
select:{"table":"category","projection":["*"]};
```
```Fly
select:{"table":"video","projection":["*"]};
```
### update table video
```Fly
update:{"table":"video",
        "filter":["country_code = US","id = 2kyS6SvSYSE"],
        "fields":["views","likes"],
        "values":["748375","57528"]};
```
### delete records from table category
```Fly
delete:{"table":"category",
        "filter":["country_code = US","id >= 10"]};
```
### truncate a table:
```Fly
clear:{"table":"category"};
```
### drop a table:
```Fly
drop:{"table":"video"};
```
### drop a database:
```Fly
drop:{"database":"youtube"};
```

### aggregation,filtering,projection:
switch to the database already loaded with data
```Fly
use:{"database":"YoutubeDemo"};
```
aggregation: the number of categories per country
```Fly
select:{"table":"category",
		"projection":["country_code","count(id)"],
		"group_by":"country_code"
};
```
aggregation: the number of trending videos per country
```Fly
select:{"table":"video",
		"projection":["country_code","count(id)"],
		"group_by":"country_code"
};
```
aggregation: the avg(views) of each channel in Britain
```Fly
select:{"table":"video",
        "projection":["channel_title","avg(likes)"],
        "filter":["country_code = GB"],
        "group_by":"channel_title"
};
```
### group by, sorting:
(Analyze the ratio of likes)

videos in US whose views > 1 million but likes< 10k, group the results by channel and then sort by dislikes

```Fly
select:{"table":"video",
        "projection":["country_code","title",
        			"channel_title","publish_time",
        			"views","likes","dislikes"],
        "filter":["country_code = US",
        		"views > 1000000",
        		"likes < 10000"],
        "group_by":"channel_title",
        "sort_by":"dislikes"
};
```
### join:
joining leads to create a temp table, and then select from the temp table
```Fly
select:{
    "join":{
    		"tables":["video","category"],
    		"on":["video.country_code = category.country_code",
    			"video.category_id = category.id"]
    },
    "projection":["video.country_code","video.title",
    			"video.likes","category.name"],
    "filter":["video.likes > 800000"],
    "group_by":"video.country_code",
    "sort_by":"",
    "limit":""
};
```

