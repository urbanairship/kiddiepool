from setuptools import setup

setup(
    name='kiddiepool',
    url='https://github.com/urbanairship/kiddiepool',
    version='1.0.0',
    license='Apache',
    author='Urban Airship',
    author_email='platform@urbanairship.com',
    description='An extensible driver framework with pooling',
    long_description=open('README.rst').read(),
    py_modules=['kiddiepool', 'test_kiddiepool'],
    install_requires=['kazoo==1.3.1'],
    tests_require=['mimic'],
    test_suite='test_kiddiepool',
    classifiers=['License :: OSI Approved :: Apache Software License'],
)
