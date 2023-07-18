import os
import threading

import config


class Logger:

    def __init__(self):
        self.lock = threading.Lock()
        file_name = config.file_log
        path = os.path.dirname(file_name)
        if not os.path.exists(path):
            os.makedirs(path)

        self.file = open(config.file_log, "a", encoding='utf-8')

    def clear(self):
        self.lock.acquire()

        self.file.seek(0)
        self.file.truncate()

        self.lock.release()

    def log(self, text):
        self.lock.acquire()

        self.file.write(text + '\n')
        self.file.flush()

        self.lock.release()
