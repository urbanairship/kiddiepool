from setuptools import setup


setup(
    name='kiddiepool',
    url='https://github.com/urbanairship/kiddiepool',
    version='0.2.0',
    license='Apache',
    author='Michael Schurter',
    author_email='schmichael@urbanairship.com',
    description='An extensible driver framework with pooling',
    long_description=open('README.rst').read(),
    py_modules=['kiddiepool', 'test_kiddiepool'],
    tests_require=['mox'],
    test_suite='test_kiddiepool',
    classifiers=['License :: OSI Approved :: Apache Software License'],
)
