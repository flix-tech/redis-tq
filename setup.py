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
    author="Team Bus Factor",
    author_email="team.bus-factor@flixbus.com",
    # FIXME!
    url='https://example.com',
    python_requires='>=3.6',
    install_requires=[
        'redis',
    ],
    packages=['redistq'],
    license='MIT',
)
