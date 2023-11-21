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
            "values":["country_code=US","id=2","name=Autos & Vehicles"]};
    insert:{"table":"category",
            "values":["country_code=US","id=10","name=Music"]};
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
    insert:{"table":"video",
            "values":["country_code=US","id=jr9QtXwC9vc","title=The Greatest Showman | Official Trailer 2 [HD] | 20th Century FOX",
                    "channel_title=20th Century Fox","category_id=1","publish_time=2017-11-13T14:00:23.000Z",
                    "views=826059","likes=3543","dislikes=119"]};
### select and projection
    //Results are grouped by partition key and sorted by sort key:
    select:{"table":"category",
            "projection":["*"]};
    select:{"table":"video",
            "projection":["*"]};
    select:{"table":"video",
            "projection":["country_code","id","title","likes"]};
### update a table
    update:{"table":"video","filter":["country_code = US","id = 2kyS6SvSYSE"],"fields":["views","likes"],"values":["748375","57528"]};
### delete records
    delete:{"table":"category","filter":["country_code = US","id >= 10"]};

### filtering and group by:
    hot videos(views>500000) from US, group the results by channel and sort by number of views

    select:{"table":"video",
            "projection":["title","channel_title","publish_time","views","likes","dislikes"],
            "filter":["country_code = US","views > 500000"],
            "group_by":"channel_title",
            "sort_by":"views"};
### aggregation: 
    the average number of views of channels in Canada

    select:{"table":"video",
            "projection":["channel_title","avg(likes)"],
            "filter":["country_code = US"],
            "group_by":"channel_title"};
### join:

    select:{
        "join":{"tables":["video","category"],"on":["video.country_code = category.country_code","video.category_id = category.id"]},
        "projection":["video.country_code","video.title","category.id","category.name"],
        "filter":[],
        "group_by":"video.country_code",
        "sort_by":"likes",
        "limit":"1"
    };
### truncate a table:
    clear:{"table":"category"};
    clear:{"table":"video"};
### drop a table:
    drop:{"table":"category"};
    drop:{"table":"video"};
### drop a database:
    drop:{"database":"youtube"};

