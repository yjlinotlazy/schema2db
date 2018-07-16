from distutils.core import setup

required_pkgs = [
    "pandas",
    "numpy"
]

setup (
    name='schema2db',
    version='0.0.1',
    packages=['schema2db'],
    install_requires=required_pkgs,
    entry_points = {
        'console_scripts': ['schema2dbdata=schema2db.gendata:main']
    }
)
