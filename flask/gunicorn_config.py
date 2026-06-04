from Logger import logger
from datetime import datetime
from gunicorn.glogging import Logger

# GUNICORN SERVER CORE CONFIGURATION
bind = "0.0.0.0:5000"
workers = 1
threads = 8
accesslog = "-"
errorlog = "-"
loglevel = "info"


worker_tmp_dir = "/dev/shm"

class CustomLogger(Logger):
    """Route Gunicorn logs through Logger.py"""
    
    def access(self, resp, req, environ, request_time):
        """Log HTTP access requests using Logger.py"""
        status = resp.status.split()[0] if resp.status else "000"
        
        logger.info(
            f'{environ.get("REMOTE_ADDR", "-")} - - '
            f'[{self._now()}] '
            f'"{environ.get("REQUEST_METHOD")} {environ.get("PATH_INFO")} {environ.get("SERVER_PROTOCOL")}" '
            f'{status} {getattr(resp, "sent", "-")} '
            f'"{environ.get("HTTP_REFERER", "-")}" '
            f'"{environ.get("HTTP_USER_AGENT", "-")}"'
        )

    def error(self, msg, *args, **kwargs):
        logger.error(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        logger.warning(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        logger.info(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        logger.debug(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        logger.critical(msg, *args, **kwargs)

    def _now(self):
        return datetime.now().strftime("%d/%b/%Y:%H:%M:%S %z")

logger_class = CustomLogger
