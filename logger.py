import os
import config


class Logger:

    def __init__(self):
        file_name = config.file_log
        path = os.path.dirname(file_name)
        if not os.path.exists(path): os.makedirs(path)
        self.file = open(file_name, "w", encoding='utf-8')

    def log(self, text):
        self.file.write(text + '\n')
        self.file.flush()
