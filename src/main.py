# -*- coding: utf-8 -*-
from src.env.task_env import create_env, parse_args
from src.etl.ads.p_cockpit_00114_data import p_cockpit_00114_data
from src.etl.ads.p_cockpit_00123_data import p_cockpit_00123_data
from src.etl.ads.p_cockpit_00124_data import p_cockpit_00124_data
from src.etl.ads.p_cockpit_00128_data import p_cockpit_00128_data
from src.etl.ads.p_cockpit_00140_data import p_cockpit_00140_data
from src.etl.dwd.P_COCKPIT_CLIENT_REVENUE import p_cockpit_client_revenue

busi_date = parse_args()
spark = create_env()

if __name__ == '__main__':
    # p_cockpit_client_revenue(spark, busi_date)

    # TODO 请实验下缓存上面的数据，然后在下面的函数中使用
    p_cockpit_00114_data(spark, busi_date)
    p_cockpit_00123_data(spark, busi_date)
    p_cockpit_00124_data(spark, busi_date)
    p_cockpit_00128_data(spark, busi_date)
    p_cockpit_00140_data(spark, busi_date)
    p_cockpit_00156_data(spark, busi_date)
    p_cockpit_00208_data(spark, busi_date)