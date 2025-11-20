import logging

# ANSI коды цветов
COLOR_CODES = {
    "DEBUG": "\033[94m",     # синий
    "INFO": "\033[92m",      # зелёный
    "WARNING": "\033[93m",   # жёлтый
    "ERROR": "\033[91m",     # красный
    "CRITICAL": "\033[95m",  # пурпурный
}
RESET_CODE = "\033[0m"

class ColoredFormatter(logging.Formatter):
    def format(self, record):
        color = COLOR_CODES.get(record.levelname, "")
        message = super().format(record)
        return f"{color}{message}{RESET_CODE}"

def get_logger(name="neo_loader", log_file="neo_loader.log", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # чтобы не дублировались сообщения

    # проверяем, есть ли уже обработчики
    if not logger.handlers:
        # консольный обработчик
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(ColoredFormatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(ch)

        # файловый обработчик
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(fh)

    return logger