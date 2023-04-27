package Beans

case class StartLog(
                     // 主键: start_time_mid
                     var id:String,
                     //根据kafka映射得到
                     open_ad_ms:String,
                     os:String,
                     ch:String,
                     is_new:String,
                     mid:String,
                     open_ad_id:String,
                     vc:String,
                     ar:String,
                     uid:String,
                     entry:String,
                     open_ad_skip_ms:String,
                     md:String,
                     loading_time:String,
                     ba:String,
                     ts:String,
                     //额外添加字段
                     var start_date:String,
                     var start_time:String
                   )
