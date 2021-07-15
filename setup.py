import setuptools

setuptools.setup(
    name="smax",
    version="1.0.1",
    install_requires=['numpy', 'pytest', 'redis', 'hiredis'],
    packages=setuptools.find_packages()
)
