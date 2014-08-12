curl -XPUT "http://localhost:9200/campaigns/" -d '{
    "settings" : {      
            "number_of_shards" : 1,
            "number_of_replicas" : 0
    },
    "mappings" : {
        "ads" : {
            "_source" : { 
                "enabled" : true
            },
            "_all" : {"enabled" : false},
            "properties" : {
                "_id" : { "type" : "string", "index" : "not_analyzed" },
                "_updated" : { "type" : "date", "format":"date_time_no_millis","index" : "not_analyzed"},
                "_created" : { "type" : "date", "format":"date_time_no_millis","index" : "not_analyzed"},
                "locations" : { "type" : "string", "index" : "not_analyzed" },
                "languages" : { "type" : "string", "index" : "not_analyzed" },
                "excluded_locations" : { "type" : "string", "index" : "not_analyzed" },                
                "status" : { "type" : "string", "index" : "not_analyzed"},
                "ad_formats" : { "type" : "string", "index" : "not_analyzed" },          
		        "campaign" : { "type" : "string", "index" : "not_analyzed"},
                "tactic" : { "type" : "string", "index" : "no"},
                "title" : { "type" : "string", "index" : "no"},
                "description" : { "type" : "string", "index" : "no"},
                "video_url" : { "type" : "string", "index" : "no"},
                "thumbnail_url" : { "type" : "string", "index" : "no"},
		        "video_url" : { "type" : "string", "index" : "no"},
                "channel" : { "type" : "string", "index" : "no"},
                "channel_url" : { "type" : "string", "index" : "no"},
                "goal_period" : { "type" : "string", "index" : "no"},
                "goal_views" : { "type" : "integer", "index" : "no"},
                "account" : { "type" : "string", "index" : "no"}
            }
        }
    }
}'
