import logging
import logging.config
import yaml
import os
from flask import Flask, Blueprint
from flask_cors import CORS
from api.utils.core import JSONEncoder
from api.utils.scheduler import scheduler_init
from api.router import router
from api.utils.cache import cache


def create_app(config_name, config_path=None):
    app = Flask(__name__)

    CORS(
        app,
        supports_credentials=True,
        resources=r'/*',
        expose_headers=[
            'Origin',
            'Accept',
            'Content-Type',
            'Content-Disposition',
            'Access-Control-Allow-Origin'])

    # 读取配置文件
    if not config_path:
        pwd = os.getcwd()
        config_path = os.path.join(pwd, 'config/config.yaml')
    if not config_name:
        config_name = 'PRODUCTION'

    # 读取配置文件
    conf = read_yaml(config_name, config_path)
    app.config.update(conf)

    # 注册接口
    register_api(app=app, routers=router)

    # 返回json格式转换
    app.json_encoder = JSONEncoder

    # 日志文件目录
    if not os.path.exists(app.config['LOGGING_PATH']):
        os.mkdir(app.config['LOGGING_PATH'])

    # 日志设置
    with open(app.config['LOGGING_CONFIG_PATH'], 'r', encoding='utf-8') as f:
        dict_conf = yaml.safe_load(f.read())
    logging.config.dictConfig(dict_conf)

    # 读取msg配置
    with open(app.config['RESPONSE_MESSAGE'], 'r', encoding='utf-8') as f:
        msg = yaml.safe_load(f.read())
        app.config.update(msg)

    # 缓存
    cache.init_app(app, {
        "DEBUG": app.config['DEBUG'],
        "CACHE_TYPE": "simple",
        "CACHE_DEFAULT_TIMEOUT": 300
    })

    # 启动定时任务
    if app.config.get("SCHEDULER_OPEN"):
        scheduler_init(app)

    return app


def read_yaml(config_name, config_path):
    """
    config_name:需要读取的配置内容
    config_path:配置文件路径
    """
    if config_name and config_path:
        with open(config_path, 'r', encoding='utf-8') as f:
            conf = yaml.safe_load(f.read())
        if config_name in conf.keys():
            return conf[config_name.upper()]
        else:
            raise KeyError('未找到对应的配置信息')
    else:
        raise ValueError('请输入正确的配置名称或配置文件路径')


def register_api(app, routers):
    for router_api in routers:
        if isinstance(router_api, Blueprint):
            app.register_blueprint(router_api)
        else:
            try:
                endpoint = router_api.__name__
                view_func = router_api.as_view(endpoint)
                # 如果没有服务名,默认 类名小写
                if hasattr(router_api, "service_name"):
                    url = '/{}/'.format(router_api.service_name.lower())
                else:
                    url = '/{}/'.format(router_api.__name__.lower())
                if 'GET' in router_api.__methods__:
                    app.add_url_rule(
                        url,
                        defaults={
                            'key': None},
                        view_func=view_func,
                        methods=[
                            'GET',
                        ])
                    app.add_url_rule(
                        '{}<string:key>'.format(url),
                        view_func=view_func,
                        methods=[
                            'GET',
                        ])
                if 'POST' in router_api.__methods__:
                    app.add_url_rule(
                        url, view_func=view_func, methods=[
                            'POST', ])
                if 'PUT' in router_api.__methods__:
                    app.add_url_rule(
                        '{}<string:key>'.format(url),
                        view_func=view_func,
                        methods=[
                            'PUT',
                        ])
                if 'DELETE' in router_api.__methods__:
                    app.add_url_rule(
                        '{}<string:key>'.format(url),
                        view_func=view_func,
                        methods=[
                            'DELETE',
                        ])
            except Exception as e:
                raise ValueError(e)

