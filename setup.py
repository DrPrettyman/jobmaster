from setuptools import setup

# python setup.py check
# python setup.py sdist
# python setup.py bdist_wheel --universal
# twine upload dist/*

_desc = """JobMaster allows a user to quickly and simply deploy and manage a collection of tasks."""

setup(
    name='jobmaster',
    version='0.1.1',
    description=_desc,
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/DrPrettyman/jobmaster',
    author='DrPrettyman',
    author_email='joshua@blinkseo.co.uk',
    license='MIT',
    packages=['jobmaster'],
    install_requires=[
        'pg8000',
        'sqlalchemy'
    ],
    classifiers=[
        "Intended Audience :: Developers",
        'Development Status :: 1 - Planning',
        'License :: OSI Approved :: MIT License',
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ]
)
