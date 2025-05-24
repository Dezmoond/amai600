import logging

def setup_logger():
    logger = logging.getLogger("data_collector")
    logger.setLevel(logging.INFO)
    
    # Prevent adding duplicate handlers
    if not logger.handlers:
        file_handler = logging.FileHandler("data_collector.log", encoding='utf-8')
        file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_format)
        
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(file_format)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger
