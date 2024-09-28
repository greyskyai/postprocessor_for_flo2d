import logging
import sys
import time

class TimingLogger:
    def __init__(self, logger):
        self.logger = logger
        self.start_time = time.time()
        self.last_log_time = self.start_time

    def log(self, message):
        current_time = time.time()
        elapsed = current_time - self.last_log_time
        total_elapsed = current_time - self.start_time
        self.logger.info(f"{message} (Step time: {elapsed:.2f}s, Total time: {total_elapsed:.2f}s)")
        self.last_log_time = current_time

def setup_logger(name='FLO2D_Postprocessor', level=logging.INFO, log_file=None):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

logger = setup_logger()
timing_logger = TimingLogger(logger)
