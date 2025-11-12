from setuptools import setup, find_namespace_packages


def _read(f):
    """
    Reads in the content of the file.
    :param f: the file to read
    :type f: str
    :return: the content
    :rtype: str
    """
    return open(f, 'rb').read()


setup(
    name="kasperl",
    description="seppl-based Python library with generic plugins for pipelines.",
    long_description=(
            _read('DESCRIPTION.rst') + b'\n' +
            _read('CHANGES.rst')).decode('utf-8'),
    url="https://github.com/waikato-datamining/kasperl",
    classifiers=[
        'Development Status :: 4 - Beta',
        'Topic :: Utilities',
        'Programming Language :: Python :: 3',
    ],
    license='MIT License',
    package_dir={
        '': 'src'
    },
    packages=find_namespace_packages(where='src'),
    install_requires=[
        "seppl>=0.3.0",
        "python-dotenv",
        "watchdog",
        "croniter",
    ],
    entry_points={
        "class_lister": [
            "kasperl=kasperl.class_lister",
        ],
    },
    version="0.0.1",
    author='Peter Reutemann',
    author_email='fracpete@waikato.ac.nz',
)
