import setuptools

setuptools.setup(
    name="smax",
    version="1.0.3",
    install_requires=['numpy', 'pytest', 'redis>=3.5.3', 'hiredis', 'psutil'],
    packages=setuptools.find_packages(),
    scripts=['smax/cli.py'],
    entry_points = {
    'console_scripts': ['smax=smax.cli:main']
    },
)
