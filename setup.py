from setuptools import setup

meta = {}
exec(open('./redistq/version.py').read(), meta)
meta['long_description'] = open('./README.md').read()

setup(
    name='redis-tq',
    version=meta['__version__'],
    description='Redis Based Task Queue',
    long_description=meta['long_description'],
    long_description_content_type='text/markdown',
    keywords='redis task queue',
    author="FlixTech",
    author_email="open-source@flixbus.com",
    url='https://github.com/flix-tech/redis-tq',
    project_urls={
        "Changelog": "https://github.com/flix-tech/redis-tq/blob/master/CHANGELOG.md",  # noqa
    },
    python_requires='>=3.7',
    install_requires=[
        'redis',
    ],
    packages=['redistq'],
    license='MIT',
)
