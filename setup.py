"""A setuptools based setup module.
See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='core.scheduler',
    version='1.0.1',
    description='Core scheduler module',

    author='dogtag',
    author_email='dogtag@protonmail.com',

    # Classifiers help users find your project by categorizing it.
    #
    # For a list of valid classifiers, see
    # https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[],

    # You can just specify package directories manually here if your project is
    # simple. Or you can use find_packages().
    #
    # Alternatively, if you just want to distribute a single Python file, use
    # the `py_modules` argument instead as follows, which will expect a file
    # called `my_module.py` to exist:
    #
    #   py_modules=["my_module"],
    #
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    namespace_packages=['core', 'core.scheduler'],

    install_requires=[],

    entry_points={
        # 'console_scripts': [
        #     'event_emittor=core.events.message_broker.rabbitmq:event_emittor',
        #     'event_listener=core.events.message_broker.rabbitmq:event_listener',
        # ],
    },
)
