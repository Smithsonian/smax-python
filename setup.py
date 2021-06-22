import setuptools

install_requires = ['numpy', 'pytest', 'redis', 'hiredis']

setuptools.setup(
    name="smax",
    version="0.2",
    packages=setuptools.find_packages()
)