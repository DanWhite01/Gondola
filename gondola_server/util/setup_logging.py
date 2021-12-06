import yaml
import logging.config


def setup_logging(config_path='config/logging_config.yml'):
    path = config_path
    with open(path, 'rt') as f:
        log_config = yaml.safe_load(f.read())
    print(log_config)
    logging.config.dictConfig(log_config)
