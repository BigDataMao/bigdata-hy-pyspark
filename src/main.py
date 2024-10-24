# -*- coding: utf-8 -*-
import time

from data.dictionaries.pub_date import pub_dates
from src.etl.ads.other.p_cockpit_00305_data import p_cockpit_00305_data
from src.etl.ads.right_sharing.p_cockpit_00301_data import p_cockpit_00301_data
from src.etl.ads.right_sharing.p_cockpit_00302_data import p_cockpit_00302_data
from src.etl.ads.right_sharing.p_cockpit_00303_data import p_cockpit_00303_data
from src.etl.ads.right_sharing.p_cockpit_00304_data import p_cockpit_00304_data
from src.etl.ads.right_sharing.p_cockpit_00306_data import p_cockpit_00306_data
from tests.gen_date import gen_date_str
from src.env.task_env import create_env, parse_args, logger
from src.etl.ads.IB_collaboration.p_cockpit_00125_data import p_cockpit_00125_data
from src.etl.ads.IB_collaboration.p_cockpit_00158_data import p_cockpit_00158_data
from src.etl.ads.IB_office.p_cockpit_00140_data import p_cockpit_00140_data
from src.etl.ads.IB_office.p_cockpit_00160_data import p_cockpit_00160_data
from src.etl.ads.OTC_options.p_cockpit_00161_data import p_cockpit_00161_data
from src.etl.ads.OTC_options.p_cockpit_00208_data import p_cockpit_00208_data
from src.etl.ads.departmental_data_adjustments.p_cockpit_00114_data import p_cockpit_00114_data
from src.etl.ads.departmental_data_adjustments.p_cockpit_00300_data import p_cockpit_00300_data
from src.etl.ads.insurance_fund.p_cockpit_00128_data import p_cockpit_00128_data
from src.etl.ads.insurance_fund.p_cockpit_00165_data import p_cockpit_00165_data
from src.etl.ads.investment_consulting.p_cockpit_00123_data import p_cockpit_00123_data
from src.etl.ads.investment_consulting.p_cockpit_00124_data import p_cockpit_00124_data
from src.etl.ads.investment_consulting.p_cockpit_00166_data import p_cockpit_00166_data
from src.etl.ads.special_client_revenue.p_cockpit_00156_data import p_cockpit_00156_data
from src.etl.ads.special_client_revenue.p_cockpit_00164_data import p_cockpit_00164_data
from src.etl.dwd.p_cockpit_client_revenue import p_cockpit_client_revenue
from src.utils.logger_uitls import to_color_str

busi_date = parse_args()
spark = create_env()

# if __name__ == '__main__':
#     begin_time = time.time()
#
#     logger.info(to_color_str(f"任务开始执行，业务日期：{busi_date}", "blue"))
#     logger.info(to_color_str("首先计算台账宽表", "blue"))
#     p_cockpit_client_revenue(spark, busi_date)
#
#     # # TODO 请实验下缓存上面的数据，然后在下面的函数中使用
#
#     logger.info(to_color_str("投资咨询", "blue"))
#     p_cockpit_00123_data(spark, busi_date)
#     p_cockpit_00124_data(spark, busi_date)
#     p_cockpit_00166_data(spark, busi_date)
#
#     logger.info(to_color_str("投保基金", "blue"))
#     p_cockpit_00128_data(spark, busi_date)
#     p_cockpit_00165_data(spark, busi_date)
#
#     logger.info(to_color_str("部门间数据调整", "blue"))
#     p_cockpit_00114_data(spark, busi_date)
#     p_cockpit_00300_data(spark, busi_date)
#
#     logger.info(to_color_str("特殊客户收入", "blue"))
#     p_cockpit_00156_data(spark, busi_date)
#     p_cockpit_00164_data(spark, busi_date)
#
#     logger.info(to_color_str("场外期权", "blue"))
#     p_cockpit_00208_data(spark, busi_date)
#     p_cockpit_00161_data(spark, busi_date)
#
#     logger.info(to_color_str("IB驻点", "blue"))
#     p_cockpit_00125_data(spark, busi_date)
#     p_cockpit_00158_data(spark, busi_date)
#
#     logger.info(to_color_str("IB协同", "blue"))
#     p_cockpit_00140_data(spark, busi_date)
#     p_cockpit_00160_data(spark, busi_date)
#
#     logger.info(to_color_str("其他调整表", "blue"))
#     p_cockpit_00305_data(spark, busi_date)
#
#     logger.info(to_color_str("权益分摊", "blue"))
#     p_cockpit_00301_data(spark, busi_date)
#     p_cockpit_00302_data(spark, busi_date)
#     p_cockpit_00303_data(spark, busi_date)
#     p_cockpit_00304_data(spark, busi_date)
#     p_cockpit_00306_data(spark, busi_date)
#
#     end_time = time.time()
#     duration = end_time - begin_time
#     minutes = duration // 60
#     seconds = duration - minutes * 60
#     logger.info(to_color_str(f"任务执行完成，耗时{minutes:.0f}分{seconds:.2f}秒", "blue"))


# if __name__ == '__main__':
#     days = gen_date_str('20240801', '20240830')
#     for day in days:
#         if pub_dates[day]["TRADE_FLAG"] == "1":
#             logger.info(to_color_str(f"{day}部门间数据调整", "blue"))
#             p_cockpit_00114_data(spark, day)
#             p_cockpit_00300_data(spark, day)

if __name__ == '__main__':
    # p_cockpit_00156_data(spark, busi_date)
    p_cockpit_00164_data(spark, busi_date)

# if __name__ == '__main__':
#     p_cockpit_00305_data(spark, busi_date)
#     p_cockpit_00301_data(spark, busi_date)
#     p_cockpit_00302_data(spark, busi_date)
#     p_cockpit_00303_data(spark, busi_date)
#     p_cockpit_00304_data(spark, busi_date)
#     p_cockpit_00306_data(spark, busi_date)
