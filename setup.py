try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

config = {
    'description': 'subwatcher',
    'author': 'Richard Bucker',
    'url': 'https://rbucker881@github.com/rbucker881/sub-watcher.git',
    'download_url': 'https://rbucker881@github.com/rbucker881/sub-watcher.git',
    'author_email': 'richard@bucker.net',
    'version': '0.1',
    'install_requires': ['python-redis-log','python-zeromq-log','pyzmq','redis-py'],
    'packages': ['sub-watcher'],
    'scripts': [],
    'name': 'sub-watcher'
}

setup(**config)
