import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


# Parse the version from the fwa module.
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
      long_description=read('README.md'),
      long_description_content_type='text/markdown',
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
      keywords='gis FWA "Freshwater Atlas" BC "British Columbia" streams watersheds',
      author=u"Simon Norris",
      author_email='snorris@hillcrestgeo.ca',
      url='https://github.com/smnorris/fwakit',
      license='MIT',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=False,
      install_requires=read('requirements.txt').splitlines(),
      extras_require={
        'test': ['pytest', 'coverage']},
      entry_points="""
      [console_scripts]
      fwakit=fwakit.cli:cli
      """
      )
