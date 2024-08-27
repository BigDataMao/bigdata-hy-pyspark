# -*- coding: utf-8 -*-
from src.env.task_env import create_env, parse_args
from src.etl.dwd.P_COCKPIT_CLIENT_REVENUE import p_cockpit_client_revenue

busi_date = parse_args()
spark = create_env()

if __name__ == '__main__':
    p_cockpit_client_revenue(spark, busi_date)