import config


class Logger:

    def __init__(self):
        file_name = config.file_log
        self.file = open(file_name, "w")

    def log(self, text):
        self.file.write(text + '\n')
        self.file.flush()
