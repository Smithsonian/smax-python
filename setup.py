import setuptools

setuptools.setup(
    name="smax",
    version="1.0.3",
    install_requires=['numpy', 'pytest', 'psutil', 'redis>=3.5.3', 'hiredis'],
    packages=setuptools.find_packages()
)
