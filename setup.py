from distutils.core import setup

setup (
    name='schema2db',
    version='0.0.1',
    packages=['schema2db'],
    entry_points = {
        'console_scripts': ['schema2dbdata=schema2db.gendata:main']
    }
)
