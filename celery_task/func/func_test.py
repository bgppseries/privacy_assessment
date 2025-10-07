
from metic import Run

import uuid

if __name__ == '__main__':
    global_uuid = str(uuid.uuid4())  ##定义全局变量uuid数据
    print("uuid为：", global_uuid)

    # Nums = ["user_billing_data_0","user_billing_data_1","user_billing_data_2","user_billing_data_3","user_billing_data_4","user_billing_data_5","user_billing_data_6","user_billing_data_7","user_billing_data_8","user_billing_data_9","user_billing_data_10","user_billing_data_11","user_billing_data_12","user_billing_data_13","user_billing_data_14"]
    # "user_traffic_data_0", "user_traffic_data_0", "user_traffic_data_0", "user_traffic_data_0",
    Nums = ["hotel_1_7_2"]
    # Nums = ["user_billing_data_3_1_3_0"]
    for eachNum in Nums[:]:
        print(eachNum)
        # 套餐id  套餐月租  账户余额
        # Run(2, 2, 0.95, 'mysql+pymysql://root:123456@localhost:3306/basicinfo', eachNum, global_uuid,
        #     ["user_id",  "id_number", "mobile_phone_number", "subscription_time"], ["package_id", "package_monthly_rent", "account_balance"], "", "", "")


        # Run(2, 2, 0.95, 'mysql+pymysql://root:123456@localhost:3306/bill_data', eachNum, global_uuid,
        #     ["bill_id", "user_id",  "mobile_phone_number", "deduct_time"], ["extra_fee", "package_monthly_rent", "account_balance"], "", "", "")

        # Run(2, 2, 0.95, 'mysql+pymysql://root:123456@localhost:3306/call_data', eachNum, global_uuid,
        #     ["user_id", "calling_party_number", "called_party_number", "talk_time"], ["call_duration"], "", "", "")

        #  充值前余额    充值金額
        # Run(2, 2, 0.95, 'mysql+pymysql://root:123456@localhost:3306/recharge_data', eachNum, global_uuid,
        #     ["order_id", "user_id", "mobile_phone_number", "recharge_time"], ["account_balance_before_recharge","recharge_amount"], "", "", "")

        ## 短信发送者所在城市  接受者所在城市
        # Run(2, 2, 0.95, 'mysql+pymysql://root:123456@localhost:3306/sms_data', eachNum, global_uuid,
        #     ["user_id", "sender_number", "receiver_number","sending_time"], ["sender_city","receiver_city"], "", "", "")

        # 还不能同时将源IP和目标IP作为准标识符，这样即使完全泛化也会因为两者的长度出现大小为1的等价类
        # user_traffic_data_13   删除比例 0.0009      user_traffic_data_14   删除比例 0.00125
        # Run(2, 2, 0.95, 'mysql+pymysql://root:123456@localhost:3306/traffic_data', eachNum, global_uuid,
        #     ["date_time", "pppoe_account", "src_ip","method"], ["app_id"], "", "", "")


        ##  敏感属性： 具体城市   准标识符：ID，时间，省份
        ##当敏感属性也被泛化时，重识别的代码逻辑就出现问题了，或者说公式出现问题。等家类种类越少越好，敏感属性种类越多越好
        # Run(2, 2, 0.95, 'mysql+pymysql://root:123456@localhost:3306/location_data', eachNum, global_uuid,
            # ["user_id", "city_at_particular_time", "specific_time"], ["province_at_specific_time"], "", "", "")

        Run(2, 2, 0.95, 'mysql+pymysql://root:784512@192.168.1.107:3306/local_test', eachNum, global_uuid,
            ["age", "id_number", "phone"], ["day_start","day_end","hotel_name"], "", "", "")


    # Run(2, 2, 0.95, 'mysql+pymysql://root:784512@192.168.1.101:3306/local_test', "user_basic_info_7", global_uuid,
    #     ["sex", "age", "id_number", "mobile_phone_number"], ["package_id", "package_monthly_rent", "subscription_time"],
    #     "", "", "")

    # Config(2,2,0.95,"","./data/csv/adult_with_pii.csv",global_uuid,"","","","","").Run()
    print("********************************************************8")