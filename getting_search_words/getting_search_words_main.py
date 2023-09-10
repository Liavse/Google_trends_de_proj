from def_getting_keyword import getting_keywords
from common.config_class import Config
from common.logger import setup_logger

config = Config()
logger = setup_logger()

username = input("Hi there, please enter you name: ")
getting_keywords(username)
logger.info("The API Process has been completed successfully")
print("The Process has been completed successfully")