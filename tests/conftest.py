from dotenv import load_dotenv
from xqute.utils import logger

load_dotenv()
logger.setLevel("DEBUG")

BUCKET = "gs://handy-buffer-287000.appspot.com"
