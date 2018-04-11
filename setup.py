from setuptools import setup, find_packages

zookeeper_extras = ['kazoo==2.4.0']
tests_require = ['mock'] + zookeeper_extras

setup(
    name='kiddiepool',
    url='https://github.com/urbanairship/kiddiepool',
    version='2.0.0',
    license='Apache',
    author='Urban Airship',
    author_email='platform@urbanairship.com',
    description='An extensible driver framework with pooling',
    long_description=open('README.rst').read(),
    packages=find_packages(),
    tests_require=tests_require,
    extras_require={
        'zookeeper': zookeeper_extras,
    },
    test_suite='tests',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python',
        'Topic :: Software Development',
    ],
)
