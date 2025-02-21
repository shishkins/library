import os
import site
from pathlib import Path

print("Создание пакета fcntl...")

path = Path(site.getsitepackages()[-1]).joinpath("fcntl")
os.makedirs(path, exist_ok=True)

code = """
def fcntl(fd, op, arg=0):
    return 0


def ioctl(fd, op, arg=0, mutable_flag=True):
    if mutable_flag:
        return 0
    else:
        return ""


def flock(fd, op):
    return


def lockf(fd, operation, length=0, start=0, whence=0):
    return
"""

with open(path.joinpath("__init__.py"), "w") as f:
    f.write(code)

print("Проверка созданного пакета...")

try:
    from fcntl import fcntl

    print("Пакет fcntl создан!")
except Exception as e:
    print("Ошибка:", e)
