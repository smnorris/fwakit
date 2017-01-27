from codecs import open as codecs_open
from setuptools import setup, find_packages


# Get the long description from the relevant file
with codecs_open('README.md', encoding='utf-8') as f:
    long_description = f.read()

# Parse the version from the pgdb module.
with open('fwakit/__init__.py', 'r') as f:
    for line in f:
        if line.find("__version__") >= 0:
            version = line.split("=")[1].strip()
            version = version.strip('"')
            version = version.strip("'")
            break

setup(name='fwakit',
      version=version,
      description=u"Python / PostgreSQL tools for working with BC Freshwater Atlas",
      long_description=long_description,
      classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
      ],
      keywords='',
      author=u"Simon Norris",
      author_email='snorris@hillcrestgeo.ca',
      url='https://github.com/smnorris/fwakit',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      dependency_links=['http://github.com/smnorris/pgdb/tarball/master#egg=pgdb-0.0.3'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'sqlalchemy',
          'click'
        ],
      extras_require={
          'test': ['pytest', 'pandas'],
      },
      entry_points="""
      [console_scripts]
      fwakit=fwakit.scripts.cli:cli
      """
      )
