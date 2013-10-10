#!/usr/bin/env python
"""
Flask-Cache
-----------

Adds cache support to your Flask application

"""

from setuptools import setup

setup(
    name='Flask-ThriftClient',
    version='0.1',
    url='http://bitbucket.org/chub/FIXME',
    license='BSD',
    author='Pierre Lamot',
    author_email='pierre.lamot@yahoo.fr',
    description='Adds thrift client support to your Flask application',
    long_description=__doc__,
    packages=[
        'flask_thriftclient',
    ],
    zip_safe=False,
    platforms='any',
    install_requires=[
        'Flask',
		'thrift>=9.1'
    ],
    classifiers=[
        'Development Status :: 1 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)

