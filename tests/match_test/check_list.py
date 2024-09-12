check_list = [
    [
        "cf_busimg.t_cockpit_client_revenue",
        "ddw.t_cockpit_client_revenue",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00114",
        "ddw.t_cockpit_00114",
        "1=1"
    ],
    [
        "cf_busimg.t_cockpit_00114_data",
        "ddw.t_cockpit_00114_data",
        "busi_date between '20240801' and '20240831'"
    ],
    [
        "cf_busimg.t_cockpit_00202",  # 依赖00300依赖的参数表
        "ddw.t_cockpit_00202",
        "1=1"
    ],
    [
        "cf_busimg.t_cockpit_00300",  # 依赖00114_data整个月份的数据
        "ddw.t_cockpit_00300",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00123",
        "ddw.t_cockpit_00123",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00124",
        "ddw.t_cockpit_00124",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00166",
        "ddw.t_cockpit_00166",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00128_data",  # 只有6月数据
        "ddw.t_cockpit_00128_data",
        "month_id = '202406'"
    ],
    [
        "cf_busimg.t_cockpit_00165",  # 依赖00128_data数据
        "ddw.t_cockpit_00165",
        "month_id = '202406'"
    ],
    [
        "cf_busimg.t_cockpit_00156",
        "ddw.t_cockpit_00156",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00164",  # 依赖00156数据
        "ddw.t_cockpit_00164",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00208",
        "ddw.t_cockpit_00208",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00161",  # 依赖00208数据
        "ddw.t_cockpit_00161",
        "month_id = '202408'"
    ],
    [
        "cf_busimg.t_cockpit_00140",
        "ddw.t_cockpit_00140",
        "month_id = '202406'"
    ],
    [
        "cf_busimg.t_cockpit_00160",  # 依赖00140数据
        "ddw.t_cockpit_00160",
        "month_id = '202406'"
    ],
    [
        "cf_busimg.t_cockpit_00125",
        "ddw.t_cockpit_00125",
        "month_id = '202406'"
    ],
    [
        "cf_busimg.t_cockpit_00158",  # 依赖00125数据
        "ddw.t_cockpit_00158",
        "month_id = '202406'"
    ],
]
