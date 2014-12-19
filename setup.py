from setuptools import setup
from setuptools import find_packages

packages = find_packages(exclude=['tests', 'integration_tests'])
setup(
    name='war',
    version='0.1.0',
    author='Guilherme Trein',
    author_email='i@trein.me',
    url='http://www.trein.me/',
    description='Simple Multiprocessing Library',
    long_description=open('README.md').read(),
    license='none',
    platforms='all',
    package_dir={'war': 'war'},
    packages=packages,
)
