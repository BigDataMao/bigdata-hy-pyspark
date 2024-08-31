# -*- coding: utf-8 -*-

import logging
import os

from lib import yaml
from src.utils.path_utils import get_project_path

# config_file = get_project_path() + "/configs/config.yaml" TODO: Fix this
config_file = "/opt/workspace/spark/bigdata-hy-pyspark/configs/config.yaml"


def load_config(config_path):
    """Load the YAML configuration file."""
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


class Config:
    _instance = None  # 类变量，用于存储单例实例
    _logger = None  # 类变量，用于存储日志记录器实例

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self, filename=config_file):
        self.config = load_config(filename)

    def get(self, key):
        return self.config.get(key)

    def get_logger(self):
        if Config._logger is not None:
            return Config._logger

        log_config = self.get("log")
        log_file_name = log_config.get("log_file_name")
        log_dir = log_config.get("log_dir")

        # 日志级别
        file_level = log_config.get("file_level")
        console_level = log_config.get("console_level")
        min_level = log_config.get("min_level")  # 设置日志器的级别为最低级别
        # 日志格式
        file_formatter = log_config.get("file_formatter")
        console_formatter = log_config.get("console_formatter")

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = str(os.path.join(log_dir, log_file_name))

        logger = logging.getLogger("logger")
        logger.setLevel(min_level)

        # 创建控制台处理程序
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(logging.Formatter(console_formatter, "%y-%m-%d %H:%M:%S"))

        # 创建文件处理程序
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(logging.Formatter(file_formatter, "%y-%m-%d %H:%M:%S"))

        # 添加处理程序到日志记录器
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

        Config._logger = logger
        return logger
