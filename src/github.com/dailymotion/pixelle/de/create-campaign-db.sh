curl -XPUT "http://$1:9200/campaigns/" -d '{
    "settings" : {      
            "number_of_shards" : 1,
            "number_of_replicas" : 0
    },
    "mappings" : {
        "ads" : {
            "_source" : { 
                "enabled" : true
            },
            "properties" : {
                "_id" : { "type" : "string", "index" : "not_analyzed" },
                "locations" : { "type" : "string", "index" : "not_analyzed" },
                "languages" : { "type" : "string", "index" : "not_analyzed" },
                "excluded_locations" : { "type" : "string", "index" : "not_analyzed" },                
                "paused_ad" : { "type" : "boolean", "index" : "not_analyzed"},                
                "paused_campaign" : { "type" : "boolean", "index" : "not_analyzed"},                
                "status" : { "type" : "string", "index" : "not_analyzed"},
                "ad_formats" : { "type" : "string", "index" : "not_analyzed" },          
                "_updated_ad" : { "type" : "date", "format":"date_time_no_millis","index" : "not_analyzed"},
                "_updated_campaign" : { "type" : "date", "format":"date_time_no_millis","index" : "not_analyzed"},
                "_created" : { "type" : "date", "format":"date_time_no_millis","index" : "not_analyzed"},
		        "campaign" : { "type" : "string", "index" : "not_analyzed", "include_in_all": false},								                   
                "tactic" : { "type" : "string", "index" : "no", "include_in_all": false},				
                "title" : { "type" : "string", "index" : "no", "include_in_all": false},
                "description" : { "type" : "string", "index" : "no", "include_in_all": false},                
                "video_url" : { "type" : "string", "index" : "no", "include_in_all": false},
                "thumbnail_url" : { "type" : "string", "index" : "no", "include_in_all": false},                
		        "video_url" : { "type" : "string", "index" : "no", "include_in_all": false},                
                "channel" : { "type" : "string", "index" : "no", "include_in_all": false},
                "channel_url" : { "type" : "string", "index" : "no", "include_in_all": false},                
                "goal_period" : { "type" : "string", "index" : "no", "include_in_all": false},  
                "goal_views" : { "type" : "integer", "index" : "no", "include_in_all": false},  
                "account" : { "type" : "string", "index" : "no", "include_in_all": false}                
            }
        }
    }
}'
