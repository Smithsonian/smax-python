import setuptools

setuptools.setup(
    name="smax",
    version="0.2",
    install_requires=['numpy', 'pytest', 'redis', 'hiredis'],
    packages=setuptools.find_packages()
)
