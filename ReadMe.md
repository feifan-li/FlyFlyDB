# FlyFlyDB CLI commands syntax
### create a database:
    create:{"database":"youtube"};
### switch a database:
    use:{"database":"youtube"};
### create a table:
    create:{"table":"category",
            "partition_key":"string country_code",
            "sort_key":"int32 id",
            "fields":["string name"],
            "partitions":"2"};
    
    create:{"table":"video",
            "partition_key":"string country_code",
            "sort_key":"string id",
            "fields":["string title","string channel_title",
                "int32 category_id","string publish_time",
                "int64 views","int64 likes","int64 dislikes"],
            "partitions":"2"};
### insert
    insert:{"table":"category",
            "values":["country_code=US","id=1","name=Film & Animation"]};
    insert:{"table":"category",
            "values":["country_code=US","id=15","name=Pets & Animals"]};


    insert:{"table":"video",
            "values":["country_code=US","id=2kyS6SvSYSE","title=WE WANT TO TALK ABOUT OUR MARRIAGE",
                    "channel_title=CaseyNeistat","category_id=22","publish_time=2017-11-13T17:13:01.000Z",
                    "views=748374","likes=57527","dislikes=2966"]};
    insert:{"table":"video",
            "values":["country_code=US","id=gHZ1Qz0KiKM","title=2 Weeks with iPhone X",
                    "channel_title=iJustine","category_id=28","publish_time=2017-11-13T19:07:23.000Z",
                    "views=119180","likes=9763","dislikes=511"]};
### select
    select:{"table":"category",
            "projection":["*"]};
    select:{"table":"video",
            "projection":["*"]};
### update table video
    update:{"table":"video","filter":["country_code = US","id = 2kyS6SvSYSE"],"fields":["views","likes"],"values":["748375","57528"]};
### delete records from table category
    delete:{"table":"category","filter":["country_code = US","id >= 10"]};
### truncate a table:
    clear:{"table":"category"};
### drop a table:
    drop:{"table":"video"};
### drop a database:
    drop:{"database":"youtube"};

### aggregation,filtering,projection:
    //switch to a database already loaded with data
    use:{"database":"YoutubeDemo"};
    //the number of categories per country
    select:{"table":"category","projection":["country_code","count(id)"],"group_by":"country_code"};
    //the number of trending videos per country
    select:{"table":"video","projection":["country_code","count(id)"],"group_by":"country_code"};
    //the average number of views of channels in Britain
    select:{"table":"video",
            "projection":["channel_title","avg(likes)"],
            "filter":["country_code = GB"],
            "group_by":"channel_title"};
### group by, sorting:
    //(Analyze the ratio of likes)
    //videos in US whose views > 1 million but likes< 10k, group the results by channel and then sort by dislikes

    select:{"table":"video",
            "projection":["country_code","title","channel_title","publish_time","views","likes","dislikes"],
            "filter":["country_code = US","views > 1000000","likes < 10000"],
            "group_by":"channel_title",
            "sort_by":"dislikes"};
### join:
    select:{
        "join":{"tables":["video","category"],"on":["video.country_code = category.country_code","video.category_id = category.id"]},
        "projection":["video.country_code","video.title","video.likes","category.name"],
        "filter":["video.likes > 1000000"],
        "group_by":"video.country_code",
        "sort_by":"",
        "limit":""
    };

