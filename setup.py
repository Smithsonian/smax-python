import setuptools

setuptools.setup(
    name="smax",
    version="1.0.1",
    install_requires=['numpy', 'pytest', 'redis>=3.5.3', 'hiredis'],
    packages=setuptools.find_packages()
)
