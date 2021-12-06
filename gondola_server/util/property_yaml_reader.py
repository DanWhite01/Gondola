from pathlib import Path
import yaml
import logging

logger = logging.getLogger(__name__)


def read_properties_yaml(properties_location):
    """
    Reads a given property file and return the values
    :param properties_location:
    :return:
    """
    properties = Path(properties_location)
    with open(properties) as file:
        logger.debug(f'Properties file found: {properties}')
        return yaml.load(file, Loader=yaml.FullLoader)
