package Beans

case class ActionLog(
                      os :String,
                      ch :String,
                      is_new :String,
                      last_page_id :String,
                      mid :String,
                      source_type :String,
                      vc :String,
                      ar :String,
                      uid :String,
                      page_id :String,
                      during_time :String,
                      md :String,
                      ba :String,
                      // 以下为对动作的说明
                      ts :Long,
                      item :String,
                      item_type :String,
                      action_id :String
                    )