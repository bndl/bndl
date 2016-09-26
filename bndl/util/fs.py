import os.path


def listdirabs(path):
    return (os.path.join(path, fname) for fname in os.listdir(path))


def read_file(filename, mode='rb'):
    with open(filename, mode) as file:
        return file.read()
