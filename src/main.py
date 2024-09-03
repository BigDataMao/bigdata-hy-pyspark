# -*- coding: utf-8 -*-
import time

from src.env.task_env import create_env, parse_args, logger
from src.etl.ads.p_cockpit_00114_data import p_cockpit_00114_data
from src.etl.ads.p_cockpit_00123_data import p_cockpit_00123_data
from src.etl.ads.p_cockpit_00124_data import p_cockpit_00124_data
from src.etl.ads.p_cockpit_00128_data import p_cockpit_00128_data
from src.etl.ads.p_cockpit_00140_data import p_cockpit_00140_data
from src.etl.ads.p_cockpit_00156_data import p_cockpit_00156_data
from src.etl.ads.p_cockpit_00208_data import p_cockpit_00208_data
from src.etl.dwd.p_cockpit_client_revenue import p_cockpit_client_revenue
from src.utils.logger_uitls import to_color_str

busi_date = parse_args()
spark = create_env()

if __name__ == '__main__':
    begin_time = time.time()
    p_cockpit_client_revenue(spark, busi_date)

    # TODO 请实验下缓存上面的数据，然后在下面的函数中使用
    p_cockpit_00114_data(spark, busi_date)
    p_cockpit_00123_data(spark, busi_date)
    p_cockpit_00124_data(spark, busi_date)
    p_cockpit_00128_data(spark, busi_date)
    p_cockpit_00140_data(spark, busi_date)
    p_cockpit_00156_data(spark, busi_date)
    p_cockpit_00208_data(spark, busi_date)

    end_time = time.time()
    duration = end_time - begin_time
    minutes = duration // 60
    seconds = duration - minutes * 60
    logger.info(to_color_str(f"任务执行完成，耗时{minutes:.0f}分{seconds:.2f}秒", "blue"))
