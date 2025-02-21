from distutils.util import convert_path

from setuptools import find_packages
from setuptools import setup

ver_dict = {}
ver_path = convert_path("db_sources/_version.py")
with open(ver_path) as ver_file:
    exec(ver_file.read(), ver_dict)

setup(
    name="db_sources",
    version=ver_dict["__version__"],
    description="Пакет для работы с различными источниками данных",
    include_package_data=True,
    packages=find_packages(),
    python_requires=">=3.10",
    zip_safe=False,
)
